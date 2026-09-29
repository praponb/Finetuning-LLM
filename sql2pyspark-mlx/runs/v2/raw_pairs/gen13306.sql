WITH OrderTotals AS (
    SELECT order_id, COUNT(DISTINCT unit_price) AS unique_orders
    FROM order_items
    WHERE unit_price <= 270
    GROUP BY order_id
)
SELECT
    o.id,
    o.status,
    o.order_date,
    O.unique_orders
FROM orders o
JOIN OrderTotals O ON o.id = O.order_id
WHERE O.unique_orders > 9
ORDER BY o.status ASC;
