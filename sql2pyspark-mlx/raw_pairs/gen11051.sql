WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(location, 'Unknown') AS location_filled
    FROM departments
    WHERE dept_name IS NOT NULL
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(location) AS location_length,
        CASE WHEN location IN ('Berlin', 'New York') THEN 'Tier 1' ELSE 'Other' END AS location_group
    FROM valid
    WHERE location IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget NULLS LAST, id) AS dedup_rank
    FROM base
    QUALIFY dedup_rank = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY location_length DESC, id) AS event_seq,
        NTILE(5) OVER (PARTITION BY dept_name, location ORDER BY budget, id) AS budget_bucket,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share,
        LEAD(budget, 1) OVER (ORDER BY budget, id) AS next_budget,
        RANK() OVER (PARTITION BY location ORDER BY budget DESC NULLS LAST) AS budget_rank
    FROM deduped
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        event_seq - budget_bucket AS event_seq_minus_budget_bucket,
        budget_bucket - dedup_rank AS budget_bucket_minus_dedup_rank
    FROM metrics
    WHERE location IS NOT NULL
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN budget_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS budget_rank_tier,
        location,
        ROUND(SUM(budget_rank) / NULLIF(COUNT(CASE WHEN location = 'Tokyo' THEN 1 END), 0), 2) AS budget_rank_per_tokyo,
        COUNT(CASE WHEN location = 'Chiang Mai' THEN 1 END) AS chiang_mai_count,
        COUNT_IF(dept_name = 'Marketing') AS marketing_events,
        GROUPING(CASE WHEN budget_rank >= 8 THEN 'HIGH' ELSE 'LOW' END) AS grouping_level
    FROM cleaned
    GROUP BY GROUPING SETS ((CASE WHEN budget_rank >= 8 THEN 'HIGH' ELSE 'LOW' END, location), (CASE WHEN budget_rank >= 8 THEN 'HIGH' ELSE 'LOW' END), ())
)
SELECT marketing_events, grouping_level
FROM grouped
ORDER BY grouping_level DESC, marketing_events;
