WITH order_stats AS (
    SELECT order_id, SUM(CASE WHEN method = 'bank_transfer' THEN amount ELSE 0 END) AS bank_transfer_amount
    FROM payments
    WHERE amount < 1200
    GROUP BY order_id
)
SELECT o.id, o.customer_id, os.bank_transfer_amount
FROM orders o
LEFT JOIN order_stats os ON o.id = os.order_id
