SELECT CASE WHEN level IN ('L2', 'L3') THEN 'priority' ELSE 'other' END level_group, SORT_ARRAY(COLLECT_SET(level)) level_list, MAX(hire_date) last_hire_date
FROM employees
GROUP BY CASE WHEN level IN ('L2', 'L3') THEN 'priority' ELSE 'other' END
ORDER BY last_hire_date
