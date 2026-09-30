SELECT HOUR(paid_at) AS paid_hour, MAX(paid_at) AS last_paid_at FROM payments WHERE order_id IS NULL OR order_id = 33 GROUP BY paid_hour ORDER BY last_paid_at ASC, paid_hour ASC LIMIT 5
