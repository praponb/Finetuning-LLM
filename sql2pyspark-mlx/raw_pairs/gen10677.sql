SELECT is_remote, SORT_ARRAY(COLLECT_SET(level)) AS level_list
FROM employees
GROUP BY is_remote;
