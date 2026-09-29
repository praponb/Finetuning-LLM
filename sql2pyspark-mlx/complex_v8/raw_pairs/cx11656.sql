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
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM policies
    WHERE start_date IS NOT NULL
),
combined_details AS (
    -- Attach holder attributes
    SELECT s.*, p.segment AS holder_segment
    FROM recent s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        premium,
        start_date,
        end_date,
        status,
        status_or_unknown,
        holder_segment,
        LAST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', start_date) ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_segment), 0), 4) AS premium_share_of_holder_segment,
        ROUND(PERCENT_RANK() OVER w_product_holder_id_by_end_dat, 3) AS end_date_percent_rank,
        SUM(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY end_date DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS premium_sum_4,
        ROUND(premium - AVG(premium) OVER w_status_or_unknown, 2) AS premium_vs_status_or_unknown_avg,
        NTILE(5) OVER (ORDER BY start_date, id) AS start_date_quintile,
        ROW_NUMBER() OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', start_date) ORDER BY end_date, id) AS rn,
        ROUND(AVG(premium) OVER (PARTITION BY holder_segment), 2) AS holder_segment_avg_premium,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY holder_segment)) AS distinct_status_in_holder_segment,
        LEAD(premium, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY end_date DESC, id) AS next2_premium,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY status_or_unknown)) AS distinct_status_in_status_or_unknown,
        NTILE(5) OVER w_end_date_month_by_end_date AS end_date_quintile,
        ROW_NUMBER() OVER (ORDER BY start_date, id) AS rn_2,
        LAST_VALUE(start_date) OVER (ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date
    FROM combined_details
    WINDOW w_product_holder_id_by_end_dat AS (PARTITION BY product, holder_id ORDER BY end_date DESC), w_status_or_unknown AS (PARTITION BY status_or_unknown), w_end_date_month_by_end_date AS (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY end_date DESC, id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        premium,
        start_date,
        end_date,
        status,
        status_or_unknown,
        holder_segment,
        last_known_premium,
        premium_share_of_holder_segment,
        end_date_percent_rank,
        premium_sum_4,
        premium_vs_status_or_unknown_avg,
        start_date_quintile,
        holder_segment_avg_premium,
        distinct_status_in_holder_segment,
        next2_premium,
        distinct_status_in_status_or_unknown,
        end_date_quintile,
        rn_2,
        last_start_date
    FROM with_windows
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    premium,
    end_date,
    status,
    status_or_unknown,
    holder_segment,
    last_known_premium,
    premium_share_of_holder_segment,
    end_date_percent_rank,
    premium_sum_4,
    premium_vs_status_or_unknown_avg,
    start_date_quintile,
    next2_premium,
    end_date_quintile,
    rn_2
FROM top_rows
WHERE distinct_status_in_status_or_unknown > 3
ORDER BY end_date DESC
