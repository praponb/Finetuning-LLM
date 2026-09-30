WITH filtered AS (
    SELECT *, CEIL(quantity) AS quantity_whole
    FROM inventory
    WHERE last_restock >= TO_DATE('2023-09-17')
      AND quantity BETWEEN 171 AND 219
),
joined AS (
    SELECT a.*, d.stock_qty
    FROM filtered a
    LEFT JOIN products d
        ON a.product_id = d.id
),
metrics AS (
    SELECT
        *,
        SUM(quantity_whole) OVER (PARTITION BY product_id) AS product_id_sum_quantity_whole,
        MAX(stock_qty) OVER (PARTITION BY store_id) AS store_id_max_stock_qty,
        AVG(stock_qty) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS stock_qty_avg_5_prior,
        RANK() OVER (ORDER BY stock_qty DESC NULLS FIRST) AS stock_qty_rank,
        FIRST_VALUE(stock_qty, TRUE) OVER (PARTITION BY store_id ORDER BY quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_stock_qty
    FROM joined
),
with_dims AS (
    SELECT
        s.id,
        s.store_id,
        s.quantity,
        s.last_restock,
        s.stock_qty,
        s.product_id_sum_quantity_whole,
        s.stock_qty_avg_5_prior,
        s.stock_qty_rank,
        p.opened_date,
        p.region,
        p.store_name
    FROM metrics s
    LEFT JOIN stores p
        ON s.store_id = p.id
    WHERE p.city IN ('Singapore', 'Bangkok', 'London')
),
enriched AS (
    SELECT
        id,
        last_restock,
        stock_qty,
        product_id_sum_quantity_whole,
        stock_qty_avg_5_prior,
        stock_qty_rank,
        opened_date,
        LEAD(stock_qty_avg_5_prior, 2) OVER (PARTITION BY store_id, region ORDER BY last_restock, id) AS next_stock_qty_avg_5_prior,
        AVG(quantity) OVER (ORDER BY stock_qty, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS quantity_avg_3_prior,
        ROUND(stock_qty_rank / SUM(stock_qty_rank) OVER (PARTITION BY region), 4) AS stock_qty_rank_share
    FROM with_dims
)
SELECT
    DATE_TRUNC('WEEK', last_restock) AS week_start,
    DATE_TRUNC('QUARTER', last_restock) AS quarter_start,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(quantity_avg_3_prior, 0.9) AS quantity_avg_3_prior_p90,
    APPROX_PERCENTILE(next_stock_qty_avg_5_prior, ARRAY(0.25, 0.5, 0.75)) AS next_stock_qty_avg_5_prior_quartiles,
    GROUPING_ID() AS grouping_level
FROM enriched
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', last_restock), DATE_TRUNC('QUARTER', last_restock)), (DATE_TRUNC('WEEK', last_restock)), ())
ORDER BY quarter_start NULLS FIRST
