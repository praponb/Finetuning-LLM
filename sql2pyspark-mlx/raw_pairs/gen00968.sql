WITH enriched AS (
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.last_restock,
        p.product_name,
        p.stock_qty
    FROM inventory s
    LEFT JOIN products p
        ON s.product_id = p.id
)
SELECT
    DATE_TRUNC('QUARTER', last_restock) AS quarter_start,
    store_id,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(stock_qty, ARRAY(0.25, 0.5, 0.75)) AS stock_qty_quartiles,
    APPROX_PERCENTILE(stock_qty, ARRAY(0.25, 0.5, 0.75)) AS stock_qty_quartiles_2,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    AVG(stock_qty) AS avg_stock_qty
FROM enriched
GROUP BY DATE_TRUNC('QUARTER', last_restock), store_id
ORDER BY avg_stock_qty DESC NULLS FIRST
