WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        location,
        budget,
        DENSE_RANK() OVER w_running AS budget_dense_rank,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location, dept_name), 4) AS budget_share,
        NTILE(10) OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS budget_bucket,
        AVG(budget) OVER w_grp AS budget_avg_5
    FROM departments
    WINDOW w_running AS (PARTITION BY dept_name ORDER BY budget NULLS LAST), w_grp AS (ORDER BY budget, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN budget_share >= 0.61 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_share_tier,
    APPROX_PERCENTILE(budget, 0.95) AS budget_p95,
    COUNT(*) FILTER (WHERE location = 'Singapore') AS singapore_rows,
    ROUND(AVG(budget_avg_5), 2) AS avg_budget_avg_5,
    SUM(CASE WHEN location = 'New York' THEN budget_dense_rank ELSE 0 END) AS new_york_budget_dense_rank,
    MAX(budget_dense_rank) FILTER (WHERE location = 'Berlin') AS berlin_max_budget_dense_rank,
    SUM(CASE WHEN location = 'Bangkok' THEN budget_bucket ELSE 0 END) AS bangkok_budget_bucket
FROM metrics
GROUP BY CASE WHEN budget_share >= 0.61 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING budget_p95 > 0
ORDER BY bangkok_budget_bucket DESC;
