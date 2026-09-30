SELECT *, SUBSTRING(level, 1, 4) AS level_prefix, CASE WHEN level IN ('L2', 'L3') THEN 'Priority' ELSE 'Other' END AS level_group FROM employees WHERE level != 'L1';
