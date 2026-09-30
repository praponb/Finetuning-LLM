WITH order_summary AS (
    SELECT order_id, MIN(amount) AS min_amount, SUM(CASE WHEN method = 'card' THEN amount ELSE 0 END) AS card_amount
    FROM payments
    GROUP BY order_id
)
SELECT o.shipping_fee, os.min_amount, os.card_amount
FROM orders o
INNER JOIN order_summary os ON o.id = os.order_id
