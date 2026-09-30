SELECT method, order_id FROM payments WHERE paid_at < '2025-01-01' INTERSECT SELECT method, order_id FROM payments WHERE method NOT IN ('promptpay', 'paypal')
