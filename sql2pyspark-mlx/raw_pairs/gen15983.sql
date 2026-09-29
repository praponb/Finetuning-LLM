WITH enriched AS (
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        CORR(stock_qty, price) OVER (PARTITION BY category) AS corr_stock_qty_price,
        NTILE(5) OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC, id ASC) AS stock_qty_bucket,
        SUM(cost) OVER (PARTITION BY supplier_id ORDER BY cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost
    FROM products
)
SELECT
    supplier_id,
    COUNT_IF(category = 'Fashion') AS fashion_events,
    COUNT(*) FILTER (WHERE brand = 'Umbrella') AS umbrella_rows,
    PERCENTILE_APPROX(stock_qty, 0.95) AS stock_qty_p95
FROM enriched
GROUP BY supplier_id
HAVING PERCENTILE_APPROX(stock_qty, 0.95) >= 0
