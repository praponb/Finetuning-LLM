SELECT
    CASE WHEN location IN ('Chiang Mai', 'Berlin') THEN 'group_a' ELSE 'other' END AS location_group,
    dept_name,
    ROUND(STDDEV(budget), 2) AS stddev_budget,
    MAX(budget) - MIN(budget) AS budget_range
FROM departments
WHERE location != 'Chiang Mai'
GROUP BY CASE WHEN location IN ('Chiang Mai', 'Berlin') THEN 'group_a' ELSE 'other' END, dept_name;
