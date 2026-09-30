WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        revenue,
        CASE WHEN revenue >= 4400 THEN 'High' WHEN revenue >= 1500 THEN 'Medium' ELSE 'Low' END AS revenue_band
    FROM store_sales
    WHERE sale_date <= '2024-05-01'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY sale_date DESC, id) AS row_seq,
        SUM(revenue) OVER (PARTITION BY product_id) AS product_id_sum_revenue,
        RANK() OVER (PARTITION BY store_id ORDER BY revenue DESC) AS revenue_rank
    FROM cleaned
    WHERE store_id = 4
),
metrics_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(row_seq) IGNORE NULLS OVER (PARTITION BY store_id, product_id ORDER BY revenue_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_row_seq,
        ROUND(revenue_rank / SUM(revenue_rank) OVER (PARTITION BY store_id, product_id), 4) AS revenue_rank_share,
        DENSE_RANK() OVER (PARTITION BY product_id, store_id ORDER BY revenue NULLS LAST) AS revenue_dense_rank
    FROM metrics
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY revenue_rank, id) AS dedup_rank
    FROM metrics_store_sales
    QUALIFY dedup_rank = 1
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN revenue_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS revenue_rank_tier,
        product_id,
        PERCENTILE_APPROX(row_seq, 0.95) AS row_seq_p95,
        MAX(revenue_dense_rank) AS max_revenue_dense_rank,
        MIN(revenue) AS min_revenue
    FROM deduped
    GROUP BY ROLLUP (CASE WHEN revenue_rank >= 8 THEN 'HIGH' ELSE 'LOW' END, product_id)
    HAVING PERCENTILE_APPROX(row_seq, 0.95) >= 0
)
SELECT
    revenue_rank_tier,
    max_revenue_dense_rank,
    min_revenue
FROM totals;
