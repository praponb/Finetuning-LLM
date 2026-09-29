SELECT id, status, order_id, TO_DATE(paid_at) AS paid_at_date FROM payments;
