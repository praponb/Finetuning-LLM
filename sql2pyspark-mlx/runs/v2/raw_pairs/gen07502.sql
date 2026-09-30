WITH order_summary AS (
    SELECT order_id, AVG(amount) avg_amount
    FROM payments
    GROUP BY order_id
)
SELECT o.id, o.channel, os.avg_amount
FROM orders o
JOIN order_summary os ON o.id = os.order_id
ORDER BY avg_amount;
