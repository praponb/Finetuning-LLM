WITH scoped_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix,
        CASE WHEN premium >= 3719.7 THEN 'large' WHEN premium >= 2033.8 THEN 'medium' ELSE 'small' END AS premium_tier
    FROM policies
    WHERE status IN ('ACTIVE', 'LAPSED', 'EXPIRED')
),
grouped AS (
    -- Group-level totals
    SELECT
        premium_tier,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        ROUND(SUM(premium), 2) AS total_premium
    FROM scoped_policys
    WHERE YEAR(end_date) = 2026
    GROUP BY premium_tier
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY distinct_holder_ids) AS rows_in_distinct_holder_ids,
        RANK() OVER (PARTITION BY distinct_holder_ids, premium_tier ORDER BY distinct_holder_ids) AS distinct_holder_ids_rank,
        NTILE(4) OVER (ORDER BY total_premium, premium_tier) AS total_premium_quartile,
        ROUND(AVG(total_premium) OVER (PARTITION BY distinct_holder_ids), 2) AS distinct_holder_ids_avg_total_premium
    FROM grouped
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(premium_tier) OVER w_distinct_holder_ids_distinct) AS distinct_premium_tier_in_distinct_holder_ids_dis,
        MIN(total_premium_quartile) OVER (PARTITION BY distinct_holder_ids, distinct_holder_ids_rank) AS distinct_holder_ids_distinct_holder_ids_rank_min,
        ROUND(total_premium_quartile - AVG(total_premium_quartile) OVER (PARTITION BY distinct_holder_ids, distinct_holder_ids_rank), 2) AS total_premium_quartile_vs_distinct_holder_ids_di,
        SUM(total_premium) OVER (PARTITION BY premium_tier ORDER BY distinct_holder_ids_avg_total_premium DESC, premium_tier ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS total_premium_sum_7
    FROM metrics
    WINDOW w_distinct_holder_ids_distinct AS (PARTITION BY distinct_holder_ids, distinct_holder_ids_rank)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        UPPER(TRIM(premium_tier)) AS premium_tier_clean,
        CONCAT(premium_tier, '#', CAST(distinct_holder_ids AS STRING)) AS premium_tier_ref,
        COALESCE(premium_tier, 'UNKNOWN') AS premium_tier_or_unknown
    FROM with_windows
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(total_premium) OVER (ORDER BY distinct_holder_ids_avg_total_premium, premium_tier ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS total_premium_max_6_prior,
        MAX(total_premium_quartile) OVER (ORDER BY distinct_holder_ids_avg_total_premium, premium_tier ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS total_premium_quartile_max_4
    FROM labeled
),
with_windows_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(distinct_holder_ids_rank) IGNORE NULLS OVER (PARTITION BY premium_tier_or_unknown, total_premium_quartile ORDER BY distinct_holder_ids_avg_total_premium DESC, premium_tier ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_distinct_holder_ids_rank,
        MIN(total_premium_sum_7) OVER (PARTITION BY total_premium_quartile, premium_tier) AS total_premium_quartile_premium_tier_min_total_pr,
        SIZE(COLLECT_SET(premium_tier_or_unknown) OVER (PARTITION BY total_premium_quartile, premium_tier)) AS distinct_premium_tier_or_unknown_in_total_premiu,
        ROUND(AVG(distinct_holder_ids) OVER (PARTITION BY premium_tier_or_unknown, total_premium_quartile ORDER BY distinct_holder_ids_avg_total_premium DESC, premium_tier ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS distinct_holder_ids_avg_2_prior
    FROM ranked
)
-- Final projection
SELECT
    premium_tier,
    distinct_holder_ids,
    total_premium,
    rows_in_distinct_holder_ids,
    distinct_holder_ids_rank,
    total_premium_quartile,
    distinct_holder_ids_avg_total_premium,
    distinct_holder_ids_distinct_holder_ids_rank_min,
    total_premium_quartile_vs_distinct_holder_ids_di,
    premium_tier_clean,
    premium_tier_or_unknown,
    total_premium_max_6_prior,
    total_premium_quartile_max_4,
    first_known_distinct_holder_ids_rank,
    total_premium_quartile_premium_tier_min_total_pr,
    distinct_holder_ids_avg_2_prior,
    GREATEST(total_premium_sum_7, total_premium) AS greatest_total_premium_sum_7_total_premium
FROM with_windows_2
ORDER BY first_known_distinct_holder_ids_rank, premium_tier
LIMIT 20
