WITH recent AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN premium >= 4417.0 THEN 'HIGH' WHEN premium >= 2289.0 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier
    FROM policies
    WHERE premium > 2289.0
      AND product IS NOT NULL
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(start_date) OVER (PARTITION BY holder_id, premium_tier ORDER BY end_date NULLS LAST, id) AS next_start_date,
        MIN(premium) OVER (PARTITION BY premium_tier) AS premium_tier_min_premium,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY premium_tier)) AS distinct_holder_id_in_premium_tier,
        RANK() OVER w_holder_id_premium_tier_by_en_2 AS end_date_rank
    FROM recent
    WINDOW w_holder_id_premium_tier_by_en_2 AS (PARTITION BY holder_id, premium_tier ORDER BY end_date NULLS LAST)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        product,
        premium,
        start_date,
        status,
        premium_tier,
        next_start_date,
        premium_tier_min_premium,
        distinct_holder_id_in_premium_tier,
        ROUND(AVG(premium) OVER (PARTITION BY premium_tier, status), 2) AS premium_tier_status_avg_premium,
        COUNT(*) OVER (PARTITION BY premium_tier, status) AS rows_in_premium_tier_status,
        LAST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY holder_id ORDER BY distinct_holder_id_in_premium_tier, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date,
        SUM(end_date_rank) OVER (PARTITION BY holder_id ORDER BY distinct_holder_id_in_premium_tier DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS end_date_rank_sum_6_prior,
        ROW_NUMBER() OVER w_holder_id_by_distinct_holder_3 AS rn
    FROM with_windows
    WINDOW w_holder_id_by_distinct_holder_3 AS (PARTITION BY holder_id ORDER BY distinct_holder_id_in_premium_tier DESC, id)
),
latest AS (
    -- Keep the latest row per premium_tier
    SELECT
        id,
        product,
        premium,
        start_date,
        status,
        premium_tier,
        next_start_date,
        premium_tier_min_premium,
        distinct_holder_id_in_premium_tier,
        premium_tier_status_avg_premium,
        rows_in_premium_tier_status,
        last_known_end_date,
        end_date_rank_sum_6_prior,
        rn
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY premium_tier ORDER BY rows_in_premium_tier_status DESC, id DESC) AS rn_2
        FROM metrics
    ) ranked
    WHERE rn_2 = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT last_known_end_date, premium_tier, end_date_rank_sum_6_prior
    FROM latest
) src
PIVOT (
    MAX(end_date_rank_sum_6_prior)
    FOR premium_tier IN ('MEDIUM', 'HIGH', 'LOW')
)
ORDER BY last_known_end_date
