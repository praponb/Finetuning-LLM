WITH order_totals AS (
    SELECT order_id, COUNT(CASE WHEN method = 'card' THEN 1 END) AS card_count, SUM(CASE WHEN status = 'FAILED' THEN amount ELSE 0 END) AS failed_amount
    FROM payments
    WHERE method NOT IN ('promptpay', 'paypal', 'bank_transfer')
    GROUP BY order_id
)
SELECT
    o.id,
    o.store_id,
    ot.card_count,
    ot.failed_amount
FROM orders o
LEFT JOIN order_totals ot ON o.id = ot.order_id;
