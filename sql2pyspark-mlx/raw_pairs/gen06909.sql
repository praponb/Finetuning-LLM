WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget DESC, id ASC) AS rn
        FROM departments
    ) ranked
    WHERE rn = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        rn + budget AS rn_plus_budget,
        CASE WHEN location IN ('Singapore', 'New York') THEN 'Priority' ELSE 'Other' END AS location_group
    FROM latest
    WHERE dept_name IS NOT NULL
      AND rn >= 1
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY rn_plus_budget DESC, id ASC) AS dedup_rank
    FROM filtered
    QUALIFY dedup_rank = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.level IN ('L4', 'L2')
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        location,
        rn_plus_budget,
        location_group,
        dedup_rank,
        LAG(rn_plus_budget) OVER (PARTITION BY location, dept_name ORDER BY rn ASC, id ASC) AS prev_rn_plus_budget,
        LEAD(budget) OVER (PARTITION BY location, dept_name ORDER BY rn ASC, id ASC) AS next_budget,
        LAG(budget) OVER (PARTITION BY location, dept_name ORDER BY rn ASC, id ASC) AS prev_budget,
        ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY rn ASC, id ASC) AS event_seq
    FROM eligible
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(rn_plus_budget AS INT) AS rn_plus_budget_int,
        COALESCE(location, 'N/A') AS location_filled
    FROM enriched
    WHERE rn_plus_budget >= 32
)
-- Running and moving aggregates
SELECT
    id,
    location,
    location_group,
    prev_rn_plus_budget,
    next_budget,
    prev_budget,
    event_seq,
    location_filled,
    AVG(event_seq) OVER (PARTITION BY location ORDER BY rn_plus_budget ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS event_seq_avg_2_prior,
    MIN(event_seq) OVER w_running AS location_min_event_seq
FROM valid
WHERE prev_rn_plus_budget BETWEEN 36 AND 75
WINDOW w_running AS (PARTITION BY location)
