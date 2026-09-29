WITH order_summary AS (
    SELECT order_id, COUNT(*) AS num_payments, SUM(amount) AS total_amount
    FROM payments
    GROUP BY order_id
)
SELECT
    o.shipping_fee,
    o.store_id,
    os.num_payments,
    os.total_amount
FROM orders o
LEFT JOIN order_summary os ON o.id = os.order_id;
