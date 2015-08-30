# -*- coding: utf-8 -*-
from __future__ import print_function

import nose

import pandas as pd
try:
    from pandas.io.pytables import HDFStore
except ImportError:
    raise nose.SkipTest('pytables not available')

import pandas.util.testing as tm

class TestPyTablesWalkGroups(tm.TestCase):
    def test_walk_groups(self):
        with tm.ensure_clean('walk_groups.hdf') as filename:
            store = HDFStore(filename, 'w')

            dfs = {
                'df1': pd.DataFrame([1,2,3]),
                'df2': pd.DataFrame([4,5,6]),
                'df3': pd.DataFrame([6,7,8]),
                'df4': pd.DataFrame([9,10,11]),
                }

            store.put('/first_group/df1', dfs['df1'])
            store.put('/first_group/df2', dfs['df2'])
            store.put('/second_group/df3', dfs['df3'])
            store.put('/second_group/third_group/df4', dfs['df4'])

            expect = {
                '/': ({'first_group', 'second_group'}, set()),
                '/first_group': (set(), {'df1', 'df2'}),
                '/second_group': ({'third_group'}, {'df3'}),
                '/second_group/third_group': (set(), {'df4'}),
            }

            for path, groups, frames in store.walk_groups():
                self.assertIn(path, expect)
                expect_groups, expect_frames = expect[path]

                self.assertEqual(expect_groups, set(groups))
                self.assertEqual(expect_frames, set(frames))
                for frame in frames:
                    frame_path = '/'.join([path, frame])
                    df = store.get(frame_path)
                    self.assert_(df.equals(dfs[frame]))
