WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(budget, 1) AS budget_rounded,
        UPPER(location) AS location_upper
    FROM departments
    WHERE budget BETWEEN 1630000 AND 3942000
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(budget) OVER w_part AS budget_avg_6_prior,
        SUM(budget_rounded) OVER (PARTITION BY location ORDER BY budget_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget_rounded
    FROM cleaned
    WINDOW w_part AS (ORDER BY budget_rounded, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.email IS NOT NULL
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_rounded,
        location_upper,
        budget_avg_6_prior,
        running_budget_rounded,
        ROW_NUMBER() OVER (ORDER BY budget_avg_6_prior DESC, id) AS row_seq,
        PERCENT_RANK() OVER (PARTITION BY dept_name ORDER BY budget NULLS FIRST) AS budget_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget_avg_6_prior, id) AS row_seq_2,
        SUM(running_budget_rounded) OVER (PARTITION BY dept_name ORDER BY budget_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_budget_rounded
    FROM eligible
)
-- Grouped metrics
SELECT
    CASE WHEN budget_avg_6_prior >= 3565267 THEN 'LARGE' ELSE 'SMALL' END AS budget_avg_6_prior_tier,
    dept_name,
    APPROX_PERCENTILE(row_seq, 0.5) AS row_seq_p50,
    ROUND(SUM(row_seq_2) / NULLIF(COUNT(CASE WHEN location = 'London' THEN 1 END), 0), 2) AS row_seq_2_per_london,
    COUNT_IF(dept_name = 'HR') AS hr_events,
    APPROX_PERCENTILE(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    PERCENTILE_APPROX(row_seq, 0.5) AS row_seq_p50_2,
    GROUPING_ID() AS grouping_level
FROM enriched
GROUP BY CUBE (CASE WHEN budget_avg_6_prior >= 3565267 THEN 'LARGE' ELSE 'SMALL' END, dept_name)
