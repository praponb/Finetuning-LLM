SELECT id, order_id, amount, TO_DATE(paid_at) AS paid_at_date FROM payments ORDER BY amount
