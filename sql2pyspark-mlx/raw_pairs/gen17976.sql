WITH order_stats AS (
    SELECT order_id, SUM(unit_price * quantity) revenue, COUNT(unit_price) unit_price_count
    FROM order_items
    WHERE order_id IN (2, 25, 33)
    GROUP BY order_id
)
SELECT
    o.id,
    o.discount,
    o.status,
    os.revenue,
    os.unit_price_count
FROM orders o
LEFT JOIN order_stats os ON o.id = os.order_id;
