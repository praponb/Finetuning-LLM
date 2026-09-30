WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date
    FROM policies
    WHERE premium BETWEEN 1051.9 AND 3544.1
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY product) AS rows_in_product,
        DENSE_RANK() OVER (ORDER BY premium) AS premium_dense_rank
    FROM filtered
),
labeled AS (
    -- Add calculated columns
    SELECT *, ROUND(premium * 0.9, 2) AS premium_adj
    FROM with_trends
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        premium_dense_rank,
        premium_adj,
        ROUND(rows_in_product / NULLIF(premium_adj, 0), 3) AS rows_in_product_per_premium_adj,
        FLOOR(premium_adj / 1000) * 1000 AS premium_adj_bucket
    FROM labeled
    WHERE product = 'HOME'
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        holder_id,
        premium_dense_rank,
        COUNT(*) AS cnt,
        ROUND(100 * COUNT(CASE WHEN product IN ('TRAVEL', 'AUTO') THEN 1 END) / COUNT(*), 1) AS pct_product_in_travel_auto,
        MIN(start_date) AS first_start_date,
        COUNT(*) FILTER (WHERE product IS NOT NULL) AS n_product_is_not_null,
        SUM(premium) AS total_premium
    FROM scored
    GROUP BY 1, 2, 3
),
with_trends_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(holder_id) OVER w_premium_dense_rank) AS distinct_holder_id_in_premium_dense_rank,
        SUM(premium_dense_rank) OVER (PARTITION BY cnt, n_product_is_not_null ORDER BY total_premium DESC, start_date_month, holder_id, premium_dense_rank ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS premium_dense_rank_sum_3,
        SUM(cnt) OVER (PARTITION BY premium_dense_rank) AS premium_dense_rank_sum_cnt
    FROM totals
    WINDOW w_premium_dense_rank AS (PARTITION BY premium_dense_rank)
)
SELECT
    n_product_is_not_null,
    cnt,
    DATEDIFF(MAX(first_start_date), MIN(first_start_date)) AS first_start_date_span_days,
    COUNT(*) AS row_count,
    ROUND(AVG(distinct_holder_id_in_premium_dense_rank), 2) AS avg_distinct_holder_id_in_premium_dense_rank
FROM with_trends_2
GROUP BY GROUPING SETS ((n_product_is_not_null, cnt), (n_product_is_not_null), (cnt), ())
ORDER BY n_product_is_not_null NULLS LAST, cnt NULLS LAST
