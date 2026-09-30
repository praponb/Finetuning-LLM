SELECT id, amount, CEIL(amount) AS amount_whole, HOUR(paid_at) AS paid_at_hour FROM payments WHERE NOT (order_id IN (23, 29, 36) AND method IN ('paypal', 'cash', 'bank_transfer'))
