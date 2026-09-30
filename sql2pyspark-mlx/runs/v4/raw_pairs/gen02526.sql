SELECT id, paid_at, amount, LAG(amount, 1) OVER (ORDER BY paid_at, id) AS prev_amount, LEAD(amount, 1) OVER (ORDER BY paid_at, id) AS next_amount FROM payments
