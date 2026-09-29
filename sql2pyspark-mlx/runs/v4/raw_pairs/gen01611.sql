SELECT
    location,
    COUNT(DISTINCT dept_name) AS distinct_dept_names,
    COUNT(*) FILTER (WHERE dept_name = 'Sales') AS sales_rows,
    PERCENTILE_APPROX(budget, 0.95) AS budget_p95
FROM departments
GROUP BY 1;
