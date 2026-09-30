SELECT order_id, method, MAX(paid_at) AS latest_paid_at FROM payments WHERE method IN ('promptpay', 'bank_transfer') AND order_id IN (22, 30) GROUP BY order_id, method;
