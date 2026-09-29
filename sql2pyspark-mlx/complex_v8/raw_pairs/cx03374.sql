WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_TRUNC('WEEK', start_date) AS start_date_week
    FROM policies
    WHERE YEAR(start_date) = 2024
),
with_details_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.status,
        p.joined_on AS holder_joined_on
    FROM scoped s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.joined_on < DATE '2025-07-03'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        premium - LAG(premium) OVER (PARTITION BY product, status ORDER BY start_date ASC, id) AS premium_change,
        NTILE(4) OVER w_all_by_premium AS premium_quartile,
        ROUND(AVG(premium) OVER w_product_holder_id, 2) AS product_holder_id_avg_premium,
        LEAD(holder_joined_on, 2) OVER w_all_by_premium AS next2_holder_joined_on,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY product, holder_id)) AS distinct_status_in_product_holder_id,
        ROUND(CUME_DIST() OVER (ORDER BY premium NULLS LAST), 3) AS premium_cume_dist,
        MIN(premium) OVER (PARTITION BY product, holder_id) AS product_holder_id_min_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER w_product_holder_id, 0), 4) AS premium_share_of_product_holder_id,
        ROUND(CUME_DIST() OVER w_all_by_start_date, 3) AS start_date_cume_dist,
        COUNT(*) OVER (PARTITION BY product, holder_id) AS rows_in_product_holder_id,
        LAG(start_date, 2) OVER w_all_by_start_date AS prev2_start_date,
        MAX(premium) OVER w_product_holder_id AS product_holder_id_max_premium
    FROM with_details_holder
    WINDOW w_all_by_premium AS (ORDER BY premium NULLS LAST, id), w_product_holder_id AS (PARTITION BY product, holder_id), w_all_by_start_date AS (ORDER BY start_date DESC, id)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', next2_holder_joined_on) AS next2_holder_joined_on_month,
        premium_quartile,
        distinct_status_in_product_holder_id,
        MIN(holder_joined_on) AS first_holder_joined_on,
        MAX(start_date) AS last_start_date,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(start_date_cume_dist), 2) AS total_start_date_cume_dist,
        MAX(product_holder_id_max_premium) AS max_product_holder_id_max_premium,
        COUNT(*) AS row_count
    FROM windowed
    GROUP BY DATE_TRUNC('MONTH', next2_holder_joined_on), premium_quartile, distinct_status_in_product_holder_id
    HAVING COUNT(*) >= 2
)
SELECT
    next2_holder_joined_on_month,
    first_holder_joined_on,
    distinct_ids,
    total_start_date_cume_dist,
    max_product_holder_id_max_premium,
    row_count
FROM stats
ORDER BY max_product_holder_id_max_premium DESC
