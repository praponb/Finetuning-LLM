WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(status)) AS status_len
    FROM policies
    WHERE premium < 4678.8
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        start_date,
        status_len,
        COUNT(*) OVER w_holder_id_status_len AS rows_in_holder_id_status_len,
        LAG(end_date, 3) OVER (PARTITION BY status_len ORDER BY status_len DESC, id) AS prev3_end_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id, product ORDER BY status_len DESC, id), 3) AS status_len_percent_rank,
        MAX(status_len) OVER w_status_product AS status_product_max_status_len,
        MAX(status_len) OVER (PARTITION BY status_len ORDER BY status_len DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS status_len_max_1_prior,
        SIZE(COLLECT_SET(holder_id) OVER w_status_product) AS distinct_holder_id_in_status_product,
        ROUND(status_len - AVG(status_len) OVER (PARTITION BY holder_id, status_len), 2) AS status_len_vs_holder_id_status_len_avg,
        SIZE(COLLECT_SET(product) OVER w_holder_id_status_len) AS distinct_product_in_holder_id_status_len
    FROM recent
    WINDOW w_holder_id_status_len AS (PARTITION BY holder_id, status_len), w_status_product AS (PARTITION BY status, product)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(start_date, prev3_end_date) AS days_prev3_end_date_to_start_date,
        ROUND(status_len_max_1_prior / NULLIF(status_len, 0), 3) AS status_len_max_1_prior_per_status_len
    FROM with_windows
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        start_date,
        rows_in_holder_id_status_len,
        prev3_end_date,
        status_len_percent_rank,
        status_product_max_status_len,
        ROUND(PERCENT_RANK() OVER w_distinct_product_in_holder_i, 3) AS start_date_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status_len, distinct_holder_id_in_status_product ORDER BY status_product_max_status_len ASC, id), 3) AS status_product_max_status_len_percent_rank,
        NTH_VALUE(status_len_percent_rank, 2) OVER (PARTITION BY status_len, distinct_product_in_holder_id_status_len ORDER BY distinct_product_in_holder_id_status_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_status_len_percent_rank,
        LAST_VALUE(status_len) OVER (PARTITION BY status_len, distinct_holder_id_in_status_product ORDER BY status_product_max_status_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_status_len,
        LEAD(status_len, 3) OVER (PARTITION BY distinct_holder_id_in_status_product ORDER BY start_date ASC, id) AS next3_status_len
    FROM annotated
    WINDOW w_distinct_product_in_holder_i AS (PARTITION BY distinct_product_in_holder_id_status_len ORDER BY start_date DESC, id)
)
SELECT
    id,
    start_date,
    prev3_end_date,
    status_len_percent_rank,
    status_product_max_status_len,
    start_date_percent_rank,
    status_product_max_status_len_percent_rank,
    second_status_len_percent_rank,
    last_status_len,
    next3_status_len,
    COALESCE(status_len_percent_rank, 0.0) AS status_len_percent_rank_filled
FROM windowed
ORDER BY second_status_len_percent_rank NULLS LAST, next3_status_len DESC
