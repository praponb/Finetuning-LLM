WITH order_summary AS (
    SELECT order_id, MIN(amount) AS min_amount
    FROM payments
    WHERE order_id IN (12, 40)
    GROUP BY order_id
)
SELECT o.shipping_fee, os.min_amount
FROM orders o
INNER JOIN order_summary os ON o.id = os.order_id
ORDER BY shipping_fee ASC;
