SELECT amount, HOUR(paid_at) AS paid_at_hour FROM payments WHERE order_id IN (2, 15, 25, 30) ORDER BY amount DESC
