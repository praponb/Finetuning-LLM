WITH order_summary AS (
    SELECT order_id, COUNT(amount) AS amount_count, COUNT(CASE WHEN method = 'promptpay' THEN 1 END) AS promptpay_count
    FROM payments
    WHERE order_id IS NULL
    GROUP BY order_id
)
SELECT
    orders.id,
    orders.customer_id,
    order_summary.amount_count,
    order_summary.promptpay_count
FROM orders
LEFT JOIN order_summary ON orders.id = order_summary.order_id
WHERE order_summary.promptpay_count > 9
ORDER BY order_summary.promptpay_count;
