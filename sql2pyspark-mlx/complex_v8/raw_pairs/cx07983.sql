WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(status)) AS status_len,
        LENGTH(TRIM(product)) AS product_len
    FROM policies
    WHERE holder_id >= 17
      AND status <> 'EXPIRED'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY product_len DESC, id) AS seq_no,
        ROUND(AVG(product_len) OVER (PARTITION BY MONTH(end_date), status_len ORDER BY status_len, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS product_len_avg_4,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY status_len, holder_id)) AS distinct_status_in_status_len_holder_id,
        ROUND(PERCENT_RANK() OVER (ORDER BY product_len DESC), 3) AS product_len_percent_rank,
        MIN(product_len) OVER (ORDER BY product_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_product_len_so_far,
        NTILE(10) OVER (ORDER BY product_len DESC, id) AS product_len_decile,
        ROUND(AVG(product_len) OVER (ORDER BY start_date ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS product_len_avg_6_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(end_date), status_len ORDER BY status_len), 3) AS status_len_cume_dist,
        NTH_VALUE(premium, 2) OVER (ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        NTILE(5) OVER (ORDER BY start_date, id) AS start_date_quintile,
        ROW_NUMBER() OVER w_end_date_month_status_len_by_3 AS status_len_seq,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), status_len ORDER BY product_len, id) AS rn,
        product_len - LAG(product_len) OVER (ORDER BY start_date ASC NULLS LAST, id) AS product_len_change
    FROM cleaned_policys
    WINDOW w_status_len_holder_id AS (PARTITION BY status_len, holder_id), w_end_date_month_status_len_by_3 AS (PARTITION BY MONTH(end_date), status_len ORDER BY status_len, id)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(product_len_change) OVER (PARTITION BY status ORDER BY rn DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS product_len_change_max_2,
        LAST_VALUE(seq_no) OVER (PARTITION BY status ORDER BY rn DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_seq_no,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY start_date_quintile)) AS distinct_holder_id_in_start_date_quintile
    FROM metrics
)
-- Final projection
SELECT
    id,
    holder_id,
    premium,
    start_date,
    end_date,
    status,
    status_len,
    product_len,
    product_len_avg_4,
    distinct_status_in_status_len_holder_id,
    min_product_len_so_far,
    product_len_decile,
    product_len_avg_6_prior,
    status_len_cume_dist,
    second_premium,
    status_len_seq,
    rn,
    product_len_change,
    product_len_change_max_2,
    last_seq_no,
    distinct_holder_id_in_start_date_quintile,
    last_seq_no + seq_no AS last_seq_no_plus_seq_no,
    MONTH(start_date) AS start_date_month
FROM windowed
