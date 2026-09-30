WITH OrderItemsByProduct AS (
    SELECT product_id, SUM(unit_price * quantity) AS revenue
    FROM order_items
    GROUP BY product_id
)
SELECT p.product_name, p.stock_qty, O.revenue
FROM products p
JOIN OrderItemsByProduct O ON p.id = O.product_id
WHERE O.revenue > 66
ORDER BY revenue
