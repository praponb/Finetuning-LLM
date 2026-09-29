WITH kept_keys AS (
    SELECT location, id
    FROM departments
    INTERSECT
    SELECT location, id
    FROM departments
    WHERE location IN ('Berlin', 'Chiang Mai', 'Bangkok')
),
verified AS (
    SELECT s.*
    FROM departments s
    JOIN kept_keys k ON s.location = k.location AND s.id = k.id
),
enriched AS (
    SELECT
        *,
        CUME_DIST() OVER w_recent AS budget_cume_dist,
        PERCENT_RANK() OVER (ORDER BY budget DESC NULLS LAST) AS budget_percent_rank
    FROM verified
    WINDOW w_recent AS (PARTITION BY location ORDER BY budget)
),
not_flagged AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.manager_id IN (9, 15)
        )
),
metrics AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_cume_dist,
        budget_percent_rank,
        AVG(budget) OVER (PARTITION BY location) AS location_avg_budget,
        DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY budget_cume_dist) AS budget_cume_dist_dense_rank,
        PERCENT_RANK() OVER w_ordered AS budget_percent_rank_2
    FROM not_flagged
    WINDOW w_ordered AS (ORDER BY budget)
)
SELECT
    location,
    CASE WHEN budget_percent_rank_2 >= 0.65 THEN 'LARGE' ELSE 'SMALL' END AS budget_percent_rank_2_tier,
    COUNT_IF(location = 'Chiang Mai') AS chiang_mai_events,
    SUM(CASE WHEN location = 'New York' THEN budget ELSE 0 END) AS new_york_budget,
    MIN(budget_percent_rank_2) AS min_budget_percent_rank_2,
    ROUND(SUM(budget_percent_rank_2) / NULLIF(COUNT(CASE WHEN dept_name = 'Sales' THEN 1 END), 0), 2) AS budget_percent_rank_2_per_sales,
    COUNT_IF(dept_name = 'HR') AS hr_events,
    COUNT_IF(dept_name = 'HR') AS hr_events_2
FROM metrics
GROUP BY location, budget_percent_rank_2_tier
HAVING new_york_budget < 0;
