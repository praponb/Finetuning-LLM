WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        CASE WHEN premium >= 3465.4 THEN 'large' WHEN premium >= 2033.8 THEN 'medium' ELSE 'small' END AS premium_tier
    FROM policies
    WHERE end_date BETWEEN DATE '2025-11-20' AND DATE '2026-09-19'
      AND product IN ('HEALTH', 'LIFE', 'AUTO')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(premium_tier) OVER (PARTITION BY status)) AS distinct_premium_tier_in_status,
        NTILE(3) OVER w_premium_tier_holder_id_by_en AS end_date_tercile,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY status), 0), 4) AS premium_share_of_status,
        ROUND(AVG(premium) OVER (PARTITION BY premium_tier, holder_id ORDER BY end_date, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS premium_avg_2,
        NTH_VALUE(premium, 2) OVER (PARTITION BY premium_tier, holder_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        FIRST_VALUE(start_date) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        LEAD(premium) OVER (PARTITION BY status, holder_id ORDER BY start_date DESC, id) AS next_premium,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY status, holder_id ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        ROW_NUMBER() OVER w_all_by_end_date AS seq_no
    FROM filtered_policys
    WINDOW w_premium_tier_holder_id_by_en AS (PARTITION BY premium_tier, holder_id ORDER BY end_date, id), w_all_by_end_date AS (ORDER BY end_date, id)
),
windowed_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(second_premium - AVG(second_premium) OVER (PARTITION BY premium_tier), 2) AS second_premium_vs_premium_tier_avg,
        ROUND(AVG(premium_share_of_status) OVER (ORDER BY first_start_date DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS premium_share_of_status_avg_4,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', end_date) ORDER BY next_premium DESC), 3) AS next_premium_cume_dist,
        ROUND(CUME_DIST() OVER w_holder_id_end_date_month_by_2, 3) AS next_premium_cume_dist_2,
        ROUND(premium_share_of_status - AVG(premium_share_of_status) OVER (PARTITION BY premium_tier), 2) AS premium_share_of_status_vs_premium_tier_avg,
        MIN(next_premium) OVER (PARTITION BY premium_tier) AS premium_tier_min_next_premium,
        ROUND(premium_avg_2 - AVG(premium_avg_2) OVER (PARTITION BY premium_tier), 2) AS premium_avg_2_vs_premium_tier_avg
    FROM windowed
    WINDOW w_holder_id_end_date_month_by_2 AS (PARTITION BY holder_id, DATE_TRUNC('MONTH', end_date) ORDER BY next_premium DESC, id)
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    end_date,
    status,
    status_slug,
    distinct_premium_tier_in_status,
    premium_avg_2,
    second_premium,
    first_start_date,
    next_premium,
    next_premium_cume_dist_2,
    premium_share_of_status_vs_premium_tier_avg,
    premium_avg_2 + next_premium AS premium_avg_2_plus_next_premium
FROM windowed_2
ORDER BY next_premium, first_start_date DESC
