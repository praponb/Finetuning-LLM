WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        LENGTH(TRIM(product)) AS product_len
    FROM policies
    WHERE start_date BETWEEN DATE '2024-10-22' AND DATE '2025-09-24'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        days_start_date_to_end_date,
        product_len,
        LEAD(premium, 2) OVER (PARTITION BY holder_id, product ORDER BY product_len ASC, id) AS next2_premium,
        ROUND(CUME_DIST() OVER w_start_date_month_by_premium, 3) AS premium_cume_dist,
        MIN(premium) OVER (PARTITION BY holder_id, status) AS holder_id_status_min_premium,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY start_date, id) AS start_date_seq,
        NTILE(4) OVER (PARTITION BY holder_id, product ORDER BY product_len ASC, id) AS product_len_quartile,
        SUM(days_start_date_to_end_date) OVER w_holder_id_status AS holder_id_status_sum_days_start_date_to_end_date,
        LAG(premium) OVER (PARTITION BY holder_id, product ORDER BY product_len ASC, id) AS prev_premium,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id, status), 2) AS premium_vs_holder_id_status_avg,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY premium DESC, id) AS premium_rank,
        SUM(product_len) OVER (PARTITION BY holder_id, status) AS holder_id_status_sum_product_len,
        ROUND(AVG(days_start_date_to_end_date) OVER (PARTITION BY holder_id, status), 2) AS holder_id_status_avg_days_start_date_to_end_date,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY holder_id, status)) AS distinct_product_in_holder_id_status,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY holder_id, product ORDER BY product_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date,
        DENSE_RANK() OVER (PARTITION BY holder_id, product ORDER BY product_len ASC) AS product_len_dense_rank,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY start_date) AS start_date_rank,
        LEAD(premium) OVER w_start_date_month_by_premium AS next_premium
    FROM prepared
    WINDOW w_start_date_month_by_premium AS (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY premium DESC, id), w_holder_id_status AS (PARTITION BY holder_id, status)
)
SELECT
    id,
    holder_id,
    premium,
    start_date,
    days_start_date_to_end_date,
    product_len,
    next2_premium,
    premium_cume_dist,
    holder_id_status_min_premium,
    start_date_seq,
    prev_premium,
    premium_rank,
    holder_id_status_sum_product_len,
    holder_id_status_avg_days_start_date_to_end_date,
    first_known_end_date,
    product_len_dense_rank,
    start_date_rank,
    CASE WHEN product IN ('AUTO', 'LIFE') THEN 'group_a' WHEN product = 'HOME' THEN 'group_b' ELSE 'other' END AS product_group,
    LENGTH(TRIM(product)) AS product_len_2
FROM metrics
ORDER BY start_date_seq, id
LIMIT 5
