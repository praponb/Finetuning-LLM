WITH order_summary AS (
    SELECT order_id, SUM(CASE WHEN method = 'promptpay' THEN amount ELSE 0 END) AS promptpay_amount
    FROM payments
    GROUP BY order_id
)
SELECT o.id, o.discount, os.promptpay_amount
FROM orders o
JOIN order_summary os ON o.id = os.order_id
ORDER BY o.discount DESC, id DESC;
