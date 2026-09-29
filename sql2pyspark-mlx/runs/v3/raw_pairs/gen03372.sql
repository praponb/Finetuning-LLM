SELECT
    location,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list,
    ROUND(STDDEV(budget), 2) AS stddev_budget,
    COUNT(budget) AS budget_count
FROM departments
GROUP BY 1
ORDER BY budget_count;
