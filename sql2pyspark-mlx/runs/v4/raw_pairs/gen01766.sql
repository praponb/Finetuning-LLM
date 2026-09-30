WITH metrics AS (
    SELECT
        *,
        FIRST_VALUE(location, TRUE) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_location,
        CUME_DIST() OVER (ORDER BY budget DESC NULLS LAST) AS budget_cume_dist
    FROM departments
)
SELECT
    dept_name,
    CASE WHEN budget_cume_dist >= 0.89 THEN 'HIGH' ELSE 'LOW' END AS budget_cume_dist_tier,
    APPROX_PERCENTILE(budget_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS budget_cume_dist_quartiles,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names,
    ROUND(AVG(budget_cume_dist), 2) AS avg_budget_cume_dist,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(budget_cume_dist, 0.95) AS budget_cume_dist_p95,
    GROUPING(dept_name) AS grouping_id
FROM metrics
GROUP BY CUBE (dept_name, CASE WHEN budget_cume_dist >= 0.89 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY budget_cume_dist_tier
