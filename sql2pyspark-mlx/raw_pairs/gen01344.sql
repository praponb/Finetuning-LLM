WITH order_stats AS (
    SELECT order_id, AVG(unit_price) AS avg_unit_price, SUM(quantity * unit_price) AS revenue
    FROM order_items
    GROUP BY order_id
)
SELECT
    o.customer_id,
    o.order_date,
    os.avg_unit_price,
    os.revenue
FROM orders o
INNER JOIN order_stats os ON o.id = os.order_id
ORDER BY os.avg_unit_price DESC, os.revenue DESC
