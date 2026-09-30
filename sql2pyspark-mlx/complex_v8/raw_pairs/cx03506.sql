WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE holder_id >= 4
      AND end_date >= DATE '2026-02-16'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY premium DESC, id) AS seq_no,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium DESC, id), 3) AS premium_percent_rank,
        DATEDIFF(end_date, LAG(end_date) OVER (ORDER BY start_date, id)) AS days_since_prev_end_date,
        SUM(premium) OVER (PARTITION BY product, holder_id ORDER BY premium_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        DATEDIFF(start_date, LAG(start_date) OVER (ORDER BY start_date, id)) AS days_since_prev_start_date,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium DESC), 3) AS premium_percent_rank_2,
        ROUND(premium - AVG(premium) OVER (PARTITION BY product), 2) AS premium_vs_product_avg,
        ROUND(PERCENT_RANK() OVER (ORDER BY start_date, id), 3) AS start_date_percent_rank,
        ROUND(AVG(premium_filled) OVER w_start_date_month_holder_id, 2) AS start_date_month_holder_id_avg_premium_filled,
        MIN(premium) OVER (PARTITION BY product) AS product_min_premium,
        RANK() OVER w_product_holder_id_by_premium_2 AS premium_filled_rank
    FROM cleaned
    WINDOW w_start_date_month_holder_id AS (PARTITION BY MONTH(start_date), holder_id), w_product_holder_id_by_premium_2 AS (PARTITION BY product, holder_id ORDER BY premium_filled, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        premium_filled,
        seq_no,
        days_since_prev_end_date,
        running_premium,
        premium_percent_rank_2,
        start_date_percent_rank,
        SUBSTRING(TRIM(product), 1, 4) AS product_prefix,
        LENGTH(TRIM(product)) AS product_len
    FROM metrics
),
metrics_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(product_len) IGNORE NULLS OVER (PARTITION BY seq_no, holder_id ORDER BY days_since_prev_end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_product_len,
        FIRST_VALUE(start_date_percent_rank) IGNORE NULLS OVER (PARTITION BY seq_no, holder_id ORDER BY days_since_prev_end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date_percent_rank,
        ROUND(days_since_prev_end_date / NULLIF(SUM(days_since_prev_end_date) OVER w_start_date_month, 0), 4) AS days_since_prev_end_date_share_of_start_date_mon,
        LAG(premium_filled, 1, 0) OVER (PARTITION BY MONTH(end_date) ORDER BY premium_percent_rank_2 DESC, id) AS prev_premium_filled,
        FIRST_VALUE(seq_no) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY running_premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_seq_no,
        SUM(days_since_prev_end_date) OVER w_start_date_month AS start_date_month_sum_days_since_prev_end_date,
        MAX(seq_no) OVER (PARTITION BY seq_no) AS seq_no_max_seq_no
    FROM annotated
    WINDOW w_start_date_month AS (PARTITION BY DATE_TRUNC('MONTH', start_date))
),
summary AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list
    FROM metrics_2
    WHERE days_since_prev_end_date >= 50
    GROUP BY DATE_TRUNC('MONTH', end_date)
)
-- Final projection
SELECT end_date_month, product_list
FROM summary
WHERE SIZE(product_list) >= 1
ORDER BY end_date_month NULLS LAST
LIMIT 20
