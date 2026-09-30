SELECT CASE WHEN budget >= 3531000 THEN 'high' WHEN budget >= 979000 THEN 'mid' ELSE 'low' END AS budget_bucket, ROUND(STDDEV(budget), 2) AS stddev_budget, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) AS location_values
FROM departments
GROUP BY 1;
