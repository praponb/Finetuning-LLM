SELECT id, order_id, paid_at, amount, SUM(amount) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS running_amount FROM payments ORDER BY running_amount DESC, paid_at
