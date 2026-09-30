SELECT id, order_id, HOUR(paid_at) AS paid_at_hour
FROM payments;
