WITH latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.order_item_unit_price
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    supplier_id,
    APPROX_PERCENTILE(stock_qty, 0.9) AS stock_qty_p90,
    SUM(stock_qty) FILTER (WHERE category = 'Home') AS home_total_stock_qty,
    COUNT(*) FILTER (WHERE category = 'Grocery') AS grocery_rows,
    PERCENTILE_APPROX(stock_qty, 0.9) AS stock_qty_p90_2
FROM latest_match
GROUP BY 1
HAVING SUM(stock_qty) FILTER (WHERE category = 'Home') > 0
ORDER BY supplier_id DESC
