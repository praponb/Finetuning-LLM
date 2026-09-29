WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
    WHERE end_date >= DATE '2026-01-20'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        status,
        LEAD(premium, 3) OVER (PARTITION BY holder_id, product ORDER BY premium DESC NULLS FIRST, id) AS next3_premium,
        SUM(premium) OVER (PARTITION BY product) AS product_sum_premium,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY product ORDER BY start_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        DATEDIFF(start_date, LAG(start_date) OVER w_holder_id_product_by_premium) AS days_since_prev_start_date,
        MAX(premium) OVER w_product AS product_max_premium,
        COUNT(*) OVER w_product AS rows_in_product,
        ROUND(AVG(premium) OVER w_product, 2) AS product_avg_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER w_product, 0), 4) AS premium_share_of_product,
        ROW_NUMBER() OVER w_holder_id_by_premium AS seq_no,
        ROUND(PERCENT_RANK() OVER w_product_by_start_date_ties, 3) AS start_date_percent_rank,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY holder_id, product ORDER BY premium DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id, product ORDER BY premium DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS premium_avg_3
    FROM filtered
    WINDOW w_holder_id_product_by_premium AS (PARTITION BY holder_id, product ORDER BY premium DESC NULLS FIRST, id), w_product AS (PARTITION BY product), w_holder_id_by_premium AS (PARTITION BY holder_id ORDER BY premium DESC, id), w_product_by_start_date_ties AS (PARTITION BY product ORDER BY start_date DESC NULLS FIRST)
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        product,
        status,
        product_sum_premium,
        days_since_prev_start_date,
        product_max_premium,
        rows_in_product,
        product_avg_premium,
        start_date_percent_rank,
        second_start_date,
        premium_avg_3,
        LEAST(product_avg_premium, seq_no) AS least_product_avg_premium_seq_no,
        UPPER(TRIM(status)) AS status_clean,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix
    FROM with_windows
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(premium_avg_3) OVER w_status_product, 2) AS status_product_avg_premium_avg_3,
        COUNT(*) OVER (PARTITION BY status, product) AS rows_in_status_product,
        NTH_VALUE(second_start_date, 2) OVER (PARTITION BY status, product ORDER BY days_since_prev_start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_start_date,
        FIRST_VALUE(rows_in_product) IGNORE NULLS OVER (PARTITION BY status ORDER BY product_sum_premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rows_in_product,
        NTILE(10) OVER w_status_by_product_sum_premiu_2 AS product_sum_premium_decile,
        LEAD(premium_avg_3) OVER w_status_product_by_days_since_2 AS next_premium_avg_3,
        SUM(days_since_prev_start_date) OVER (PARTITION BY status, product) AS status_product_sum_days_since_prev_start_date
    FROM labeled
    WINDOW w_status_product AS (PARTITION BY status, product), w_status_by_product_sum_premiu_2 AS (PARTITION BY status ORDER BY product_sum_premium, id), w_status_product_by_days_since_2 AS (PARTITION BY status, product ORDER BY days_since_prev_start_date, id)
)
-- Final projection
SELECT
    id,
    product,
    status,
    product_sum_premium,
    days_since_prev_start_date,
    product_max_premium,
    product_avg_premium,
    start_date_percent_rank,
    premium_avg_3,
    least_product_avg_premium_seq_no,
    status_clean,
    product_prefix,
    status_product_avg_premium_avg_3,
    rows_in_status_product,
    second_second_start_date,
    first_known_rows_in_product,
    product_sum_premium_decile,
    next_premium_avg_3,
    status_product_sum_days_since_prev_start_date,
    FLOOR(rows_in_status_product / 5) * 5 AS rows_in_status_product_bucket,
    ROUND(product_avg_premium * 1.1, 2) AS product_avg_premium_adj
FROM metrics
WHERE least_product_avg_premium_seq_no >= 2000.6
ORDER BY product_avg_premium_adj, first_known_rows_in_product NULLS LAST
