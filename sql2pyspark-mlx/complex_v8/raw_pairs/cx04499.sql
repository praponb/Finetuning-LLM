WITH base AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        tier,
        hired_on,
        DATE_TRUNC('YEAR', hired_on) AS hired_on_year
    FROM support_agents
    WHERE YEAR(hired_on) = 2024
      AND agent_name IS NOT NULL
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', hired_on_year) AS hired_on_year_month,
        tier,
        ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE tier IN ('T3', 'T2')) AS n_tier_in_t3_t2
    FROM base
    WHERE hired_on < DATE '2025-02-26'
    GROUP BY DATE_TRUNC('MONTH', hired_on_year), tier
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM summary
    WHERE n_tier_in_t3_t2 > (SELECT MIN(n_tier_in_t3_t2) FROM summary)
),
labeled AS (
    -- Derive helper columns
    SELECT
        hired_on_year_month,
        tier,
        FLOOR(n_tier_in_t3_t2 / 2) * 2 AS n_tier_in_t3_t2_bucket,
        COALESCE(distinct_ids, 0) AS distinct_ids_filled
    FROM qualified
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(hired_on_year_month, 3) OVER (PARTITION BY distinct_ids_filled ORDER BY distinct_ids_filled DESC, hired_on_year_month, tier) AS prev3_hired_on_year_month,
        ROUND(AVG(distinct_ids_filled) OVER (PARTITION BY distinct_ids_filled ORDER BY distinct_ids_filled DESC, hired_on_year_month, tier ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS distinct_ids_filled_avg_3,
        ROW_NUMBER() OVER (PARTITION BY distinct_ids_filled ORDER BY distinct_ids_filled DESC, hired_on_year_month, tier) AS seq_no,
        RANK() OVER (PARTITION BY distinct_ids_filled ORDER BY distinct_ids_filled DESC) AS distinct_ids_filled_rank,
        NTH_VALUE(hired_on_year_month, 2) OVER (PARTITION BY distinct_ids_filled ORDER BY distinct_ids_filled DESC, hired_on_year_month, tier ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on_year_month,
        LEAD(n_tier_in_t3_t2_bucket) OVER (PARTITION BY distinct_ids_filled ORDER BY distinct_ids_filled DESC, hired_on_year_month, tier) AS next_n_tier_in_t3_t2_bucket
    FROM labeled
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(tier, 'UNKNOWN') AS tier_or_unknown,
        ABS(seq_no - 7) AS seq_no_distance,
        DATEDIFF(DATE '2026-01-01', hired_on_year_month) AS days_since_hired_on_year_month
    FROM with_windows
    WHERE seq_no > 2
)
-- Final projection
SELECT
    hired_on_year_month,
    tier,
    n_tier_in_t3_t2_bucket,
    distinct_ids_filled,
    prev3_hired_on_year_month,
    distinct_ids_filled_avg_3,
    seq_no,
    second_hired_on_year_month,
    next_n_tier_in_t3_t2_bucket,
    tier_or_unknown,
    seq_no_distance,
    days_since_hired_on_year_month,
    CONCAT(tier, '#', CAST(distinct_ids_filled AS STRING)) AS tier_ref
FROM enriched
WHERE MONTH(hired_on_year_month) IN (1, 2)
