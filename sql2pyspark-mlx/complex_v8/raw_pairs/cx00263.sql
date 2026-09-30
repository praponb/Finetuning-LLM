WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref
    FROM policies
    WHERE (end_date < DATE '2026-10-23' OR premium < 2945.9)
),
enriched AS (
    -- Attach holder attributes
    SELECT s.*, p.holder_name
    FROM prepared s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        premium,
        end_date,
        status,
        days_start_date_to_end_date,
        holder_name,
        LAG(premium, 2) OVER w_product_by_premium AS prev2_premium,
        ROUND(days_start_date_to_end_date / NULLIF(SUM(days_start_date_to_end_date) OVER (PARTITION BY status, product), 0), 4) AS days_start_date_to_end_date_share_of_status_prod,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY status, product)) AS distinct_holder_id_in_status_product,
        LAG(days_start_date_to_end_date, 1, 0) OVER (PARTITION BY status, holder_id ORDER BY premium DESC, id) AS prev_days_start_date_to_end_date,
        LAST_VALUE(days_start_date_to_end_date) IGNORE NULLS OVER (PARTITION BY status, holder_id ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_start_date_to_end_date,
        ROUND(PERCENT_RANK() OVER w_product_by_premium, 3) AS premium_percent_rank,
        premium - LAG(premium) OVER (ORDER BY premium, id) AS premium_change,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status, DATE_TRUNC('MONTH', end_date) ORDER BY premium NULLS LAST), 3) AS premium_percent_rank_2,
        ROUND(AVG(premium) OVER (PARTITION BY product ORDER BY premium, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_1_prior,
        ROW_NUMBER() OVER (PARTITION BY status, holder_id ORDER BY premium DESC, id) AS rn,
        ROW_NUMBER() OVER (ORDER BY premium, id) AS premium_seq,
        LEAD(end_date) OVER w_status_holder_id_by_premium AS next_end_date,
        ROUND(CUME_DIST() OVER (PARTITION BY status, DATE_TRUNC('MONTH', end_date) ORDER BY premium NULLS LAST, id), 3) AS premium_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status, holder_id ORDER BY premium DESC), 3) AS premium_percent_rank_3
    FROM enriched
    WINDOW w_product_by_premium AS (PARTITION BY product ORDER BY premium, id), w_status_holder_id_by_premium AS (PARTITION BY status, holder_id ORDER BY premium DESC, id)
),
totals AS (
    -- Group-level totals
    SELECT
        distinct_holder_id_in_status_product,
        SUM(days_start_date_to_end_date_share_of_status_prod) FILTER (WHERE LENGTH(TRIM(holder_name)) >= 5) AS days_start_date_to_end_date_share_of_status_prod_2,
        COUNT(DISTINCT status) AS distinct_status
    FROM windowed
    GROUP BY distinct_holder_id_in_status_product
)
-- Final projection
SELECT
    distinct_holder_id_in_status_product,
    days_start_date_to_end_date_share_of_status_prod_2,
    distinct_status,
    ABS(distinct_status - 5) AS distinct_status_distance,
    COALESCE(distinct_status, 0) AS distinct_status_filled
FROM totals
ORDER BY distinct_status
