SELECT id, paid_at, order_id, amount, LAG(amount, 1) OVER (PARTITION BY YEAR(paid_at), order_id ORDER BY paid_at, id) AS prev_amount FROM payments
