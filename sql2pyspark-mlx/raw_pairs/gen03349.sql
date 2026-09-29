WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY dept_name, location ORDER BY budget NULLS FIRST) AS budget_rank,
        SUM(budget) OVER (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget,
        LEAD(budget, 2) OVER (PARTITION BY location ORDER BY budget, id) AS next_budget,
        AVG(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS budget_avg_3_prior,
        LEAD(budget, 2) OVER (PARTITION BY location ORDER BY budget, id) AS next_budget_2
    FROM departments
    WHERE budget IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_windows
    QUALIFY RANK() OVER (PARTITION BY location ORDER BY next_budget_2) = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(dept_name) AS dept_name_lower,
        COALESCE(dept_name, 'UNKNOWN') AS resolved_dept_name
    FROM deduped
    WHERE dept_name IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY next_budget_2, id) = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        location,
        next_budget,
        budget_avg_3_prior,
        next_budget_2,
        dept_name_lower,
        resolved_dept_name,
        LEAD(budget_rank) OVER (PARTITION BY dept_name, location ORDER BY next_budget, id) AS next_budget_rank,
        LAG(budget) OVER (PARTITION BY dept_name, location ORDER BY next_budget, id) AS prev_budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY next_budget, id) AS row_seq,
        NTILE(10) OVER (ORDER BY cumulative_budget, id) AS cumulative_budget_bucket
    FROM ranked
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(next_budget, 2) OVER w_recent AS next_next_budget,
        PERCENT_RANK() OVER w_grp AS budget_avg_3_prior_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY prev_budget DESC, id) AS event_seq
    FROM enriched
    WINDOW w_recent AS (PARTITION BY location ORDER BY next_budget_rank, id), w_grp AS (PARTITION BY location ORDER BY budget_avg_3_prior NULLS FIRST)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND LOWER(c.email) LIKE '%any%'
        )
)
-- Aggregate with conditional metrics
SELECT
    location,
    SUM(CASE WHEN location = 'London' THEN row_seq ELSE 0 END) AS london_row_seq,
    SUM(CASE WHEN location = 'Berlin' THEN budget_avg_3_prior ELSE 0 END) AS berlin_budget_avg_3_prior,
    SUM(CASE WHEN location = 'Tokyo' THEN budget_avg_3_prior ELSE 0 END) AS tokyo_budget_avg_3_prior
FROM not_flagged
GROUP BY 1
HAVING tokyo_budget_avg_3_prior > 0
