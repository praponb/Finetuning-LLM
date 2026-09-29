SELECT order_id, MIN(paid_at) AS min_paid_at
FROM payments
WHERE method IN ('promptpay', 'paypal', 'card')
GROUP BY order_id;
