WITH product_summary AS (
    SELECT product_id, MAX(unit_price) max_unit_price, MIN(quantity) min_quantity
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.id,
    p.stock_qty,
    ps.max_unit_price,
    ps.min_quantity
FROM products p
INNER JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.max_unit_price >= 210
ORDER BY id DESC, max_unit_price DESC
LIMIT 10
