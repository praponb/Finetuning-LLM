SELECT id, paid_at, amount, LEAD(amount, 1) OVER (ORDER BY paid_at, id) AS next_amount FROM payments ORDER BY next_amount;
