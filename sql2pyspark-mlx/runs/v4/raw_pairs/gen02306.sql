SELECT level, is_remote, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values, SORT_ARRAY(COLLECT_SET(level)) AS level_list FROM employees GROUP BY level, is_remote
