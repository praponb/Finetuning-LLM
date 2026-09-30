WITH scoped_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN premium >= 3719.7 THEN 'large' WHEN premium >= 1541.8 THEN 'medium' ELSE 'small' END AS premium_tier
    FROM policies
    WHERE product = 'AUTO'
      AND premium < 3719.7
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        premium_tier,
        SUM(premium) OVER (PARTITION BY holder_id ORDER BY start_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS premium_sum_3_prior,
        SUM(premium) OVER (PARTITION BY holder_id ORDER BY end_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id, premium_tier), 0), 4) AS premium_share_of_holder_id_premium_tier,
        SUM(premium) OVER w_holder_id_premium_tier AS holder_id_premium_tier_sum_premium,
        MAX(premium) OVER w_holder_id_premium_tier AS holder_id_premium_tier_max_premium,
        MIN(premium) OVER (PARTITION BY status) AS status_min_premium,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY holder_id, premium_tier ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        SIZE(COLLECT_SET(status) OVER w_holder_id_premium_tier) AS distinct_status_in_holder_id_premium_tier,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id, premium_tier), 2) AS premium_vs_holder_id_premium_tier_avg,
        FIRST_VALUE(premium) OVER (PARTITION BY holder_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium,
        ROW_NUMBER() OVER w_holder_id_by_end_date AS rn,
        NTILE(10) OVER (PARTITION BY holder_id ORDER BY end_date DESC NULLS FIRST, id) AS end_date_decile
    FROM scoped_policys
    WINDOW w_holder_id_premium_tier AS (PARTITION BY holder_id, premium_tier), w_holder_id_by_end_date AS (PARTITION BY holder_id ORDER BY end_date DESC NULLS FIRST, id)
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM with_windows
    WHERE rn <= 5
)
SELECT
    id,
    premium,
    start_date,
    end_date,
    status,
    premium_tier,
    premium_sum_3_prior,
    running_premium,
    premium_share_of_holder_id_premium_tier,
    holder_id_premium_tier_sum_premium,
    holder_id_premium_tier_max_premium,
    status_min_premium,
    second_end_date,
    distinct_status_in_holder_id_premium_tier,
    premium_vs_holder_id_premium_tier_avg,
    first_premium,
    rn,
    end_date_decile,
    COALESCE(premium, 0.0) AS premium_filled,
    CASE WHEN premium_tier IN ('small', 'medium') THEN 'group_a' WHEN premium_tier = 'large' THEN 'group_b' ELSE 'other' END AS premium_tier_group
FROM top_rows
WHERE start_date BETWEEN DATE '2024-03-15' AND DATE '2025-07-07'
ORDER BY premium_vs_holder_id_premium_tier_avg DESC, status DESC NULLS FIRST
