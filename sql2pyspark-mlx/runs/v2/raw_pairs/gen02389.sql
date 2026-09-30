WITH order_summary AS (
    SELECT order_id, COUNT(quantity) AS quantity_count, AVG(quantity) AS avg_quantity
    FROM order_items
    GROUP BY order_id
)
SELECT
    o.amount,
    o.channel,
    os.quantity_count,
    os.avg_quantity
FROM orders o
LEFT JOIN order_summary os ON o.id = os.order_id;
