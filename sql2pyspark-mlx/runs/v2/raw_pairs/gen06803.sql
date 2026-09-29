SELECT id, method, paid_at, amount, LAG(amount, 2) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_amount FROM payments ORDER BY id DESC;
