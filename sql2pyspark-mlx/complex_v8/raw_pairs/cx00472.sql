WITH valid_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date
    FROM policies
    WHERE start_date < DATE '2025-05-28'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY start_date) AS start_date_dense_rank,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id ORDER BY days_end_date_to_start_date DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS premium_avg_7,
        LEAD(days_end_date_to_start_date, 3, 0) OVER (PARTITION BY product ORDER BY premium DESC, id) AS next3_days_end_date_to_start_date,
        ROUND(premium / NULLIF(SUM(premium) OVER w_product_status, 0), 4) AS premium_share_of_product_status,
        LAG(premium, 2) OVER (ORDER BY end_date, id) AS prev2_premium,
        COUNT(*) OVER (PARTITION BY product, status) AS rows_in_product_status,
        ROUND(PERCENT_RANK() OVER w_all_by_start_date_ties, 3) AS start_date_percent_rank,
        LAG(days_end_date_to_start_date, 3) OVER w_product_by_premium AS prev3_days_end_date_to_start_date,
        LAG(premium, 3, 0) OVER (ORDER BY end_date, id) AS prev3_premium,
        ROUND(PERCENT_RANK() OVER w_all_by_premium_ties, 3) AS premium_percent_rank,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY holder_id ORDER BY days_end_date_to_start_date DESC, id)) AS days_since_prev_end_date,
        ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY days_end_date_to_start_date DESC, id) AS days_end_date_to_start_date_seq,
        MAX(premium) OVER (ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS premium_max_3,
        ROUND(premium - AVG(premium) OVER w_product_status, 2) AS premium_vs_product_status_avg
    FROM valid_policys
    WINDOW w_product_status AS (PARTITION BY product, status), w_all_by_start_date_ties AS (ORDER BY start_date), w_product_by_premium AS (PARTITION BY product ORDER BY premium DESC, id), w_all_by_premium_ties AS (ORDER BY premium DESC NULLS FIRST)
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        days_end_date_to_start_date,
        start_date_dense_rank,
        premium_avg_7,
        next3_days_end_date_to_start_date,
        premium_share_of_product_status,
        prev2_premium,
        rows_in_product_status,
        start_date_percent_rank,
        prev3_days_end_date_to_start_date,
        prev3_premium,
        premium_percent_rank,
        days_since_prev_end_date,
        premium_max_3,
        premium_vs_product_status_avg
    FROM metrics
    WHERE days_end_date_to_start_date_seq <= 3
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY YEAR(start_date), start_date_dense_rank)) AS distinct_status_in_start_date_year_start_date_de,
        FIRST_VALUE(rows_in_product_status) IGNORE NULLS OVER (PARTITION BY product ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rows_in_product_status,
        LEAD(start_date_dense_rank) OVER (PARTITION BY product ORDER BY start_date DESC, id) AS next_start_date_dense_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY start_date_percent_rank), 3) AS start_date_percent_rank_percent_rank,
        FIRST_VALUE(premium_max_3) IGNORE NULLS OVER (ORDER BY start_date_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium_max_3,
        LAST_VALUE(premium_avg_7) OVER (PARTITION BY start_date_dense_rank ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium_avg_7
    FROM first_rows
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        start_date_dense_rank,
        holder_id,
        ROUND(SUM(start_date_percent_rank), 2) AS total_start_date_percent_rank,
        MIN(end_date) AS first_end_date,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        ROUND(100 * COUNT(CASE WHEN status IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_status_is_not_null
    FROM sequenced
    GROUP BY start_date_dense_rank, holder_id
),
metrics_2 AS (
    -- Window metrics over several partitions
    SELECT
        start_date_dense_rank,
        holder_id,
        first_end_date,
        product_list,
        pct_status_is_not_null,
        SUM(start_date_dense_rank) OVER (PARTITION BY holder_id) AS holder_id_sum_start_date_dense_rank,
        MAX(start_date_dense_rank) OVER (PARTITION BY start_date_dense_rank, holder_id ORDER BY total_start_date_percent_rank, start_date_dense_rank, holder_id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS start_date_dense_rank_max_2
    FROM grouped
)
SELECT
    start_date_dense_rank,
    holder_id,
    first_end_date,
    product_list,
    pct_status_is_not_null,
    start_date_dense_rank_max_2
FROM metrics_2
WHERE first_end_date >= DATE '2026-02-16'
ORDER BY start_date_dense_rank_max_2, first_end_date, start_date_dense_rank, holder_id
LIMIT 5
