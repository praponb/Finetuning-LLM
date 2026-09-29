SELECT
    CASE WHEN budget >= 4288000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
    dept_name,
    COUNT(*) AS row_count,
    STDDEV_SAMP(budget) AS budget_stddev,
    PERCENTILE_APPROX(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    COUNT(DISTINCT dept_name) AS distinct_dept_names
FROM departments
GROUP BY CASE WHEN budget >= 4288000 THEN 'HIGH' ELSE 'LOW' END, dept_name
ORDER BY distinct_dept_names DESC
