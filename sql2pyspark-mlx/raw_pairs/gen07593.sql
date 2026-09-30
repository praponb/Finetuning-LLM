SELECT
    order_id,
    amount,
    status,
    CAST(paid_at AS DATE) AS paid_at_day
FROM payments
WHERE order_id = 19 AND method IN ('paypal', 'promptpay', 'cash')
ORDER BY order_id;
