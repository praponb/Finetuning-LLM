SELECT *
FROM payments
WHERE order_id = 34 AND (method NOT IN ('promptpay', 'card', 'paypal') OR TO_DATE(paid_at) < '2024-10-21');
