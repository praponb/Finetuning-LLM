WITH product_totals AS (
    SELECT product_id, COUNT(DISTINCT unit_price) AS unique_orders, COUNT(*) AS order_item_count
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.stock_qty,
    p.brand,
    pt.unique_orders,
    pt.order_item_count
FROM products p
INNER JOIN product_totals pt ON p.id = pt.product_id
WHERE pt.order_item_count >= 7;
