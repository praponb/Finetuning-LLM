WITH product_totals AS (
    SELECT product_id, COUNT(*) AS num_order_items, SUM(unit_price * quantity) AS revenue
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.id,
    p.brand,
    pt.num_order_items,
    pt.revenue
FROM products p
JOIN product_totals pt ON p.id = pt.product_id
ORDER BY pt.revenue DESC
