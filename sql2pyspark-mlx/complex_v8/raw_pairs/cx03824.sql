WITH recent AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        CASE WHEN product IN ('AUTO', 'TRAVEL') THEN 'group_a' WHEN product = 'HEALTH' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE (end_date < DATE '2026-08-31' OR product = 'HOME')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        DATEDIFF(start_date, LAG(start_date) OVER w_product_by_premium) AS days_since_prev_start_date,
        MIN(premium) OVER (PARTITION BY holder_id, product_group) AS holder_id_product_group_min_premium,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY holder_id, product_group)) AS distinct_status_in_holder_id_product_group,
        MAX(premium) OVER (PARTITION BY holder_id, product_group) AS holder_id_product_group_max_premium,
        ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY premium, id) AS rn,
        SIZE(COLLECT_SET(holder_id) OVER w_product_group_product) AS distinct_holder_id_in_product_group_product,
        ROUND(PERCENT_RANK() OVER (PARTITION BY product_group ORDER BY start_date, id), 3) AS start_date_percent_rank,
        ROUND(AVG(premium) OVER (PARTITION BY product_group, product), 2) AS product_group_product_avg_premium,
        COUNT(*) OVER w_product_group_product AS rows_in_product_group_product,
        ROUND(premium / NULLIF(SUM(premium) OVER w_product_group_product, 0), 4) AS premium_share_of_product_group_product,
        ROUND(AVG(premium) OVER (PARTITION BY product_group ORDER BY start_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS premium_avg_7,
        RANK() OVER w_product_group_by_start_date_3 AS start_date_rank,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id, product_group), 2) AS premium_vs_holder_id_product_group_avg,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id, product_group), 0), 4) AS premium_share_of_holder_id_product_group
    FROM recent
    WINDOW w_product_by_premium AS (PARTITION BY product ORDER BY premium DESC, id), w_product_group_product AS (PARTITION BY product_group, product), w_product_group_by_start_date_3 AS (PARTITION BY product_group ORDER BY start_date)
),
first_rows AS (
    -- Top 2 per group
    SELECT *
    FROM with_trends
    WHERE start_date_rank <= 2
),
with_trends_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY distinct_status_in_holder_id_product_group) AS rows_in_distinct_status_in_holder_id_product_gro,
        DENSE_RANK() OVER w_all_by_premium_share_of_hold AS premium_share_of_holder_id_product_group_dense_r,
        LAG(premium_avg_7) OVER (PARTITION BY rn, distinct_holder_id_in_product_group_product ORDER BY holder_id_product_group_min_premium DESC, id) AS prev_premium_avg_7
    FROM first_rows
    WINDOW w_all_by_premium_share_of_hold AS (ORDER BY premium_share_of_holder_id_product_group)
)
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    days_since_prev_start_date,
    holder_id_product_group_min_premium,
    distinct_status_in_holder_id_product_group,
    holder_id_product_group_max_premium,
    rn,
    distinct_holder_id_in_product_group_product,
    start_date_percent_rank,
    product_group_product_avg_premium,
    rows_in_product_group_product,
    premium_share_of_product_group_product,
    start_date_rank,
    premium_share_of_holder_id_product_group,
    rows_in_distinct_status_in_holder_id_product_gro,
    premium_share_of_holder_id_product_group_dense_r,
    prev_premium_avg_7
FROM with_trends_2
WHERE distinct_holder_id_in_product_group_product < 9
