SELECT id, status, paid_at, amount, LEAD(amount, 2) OVER (PARTITION BY status ORDER BY paid_at, id) AS next_amount FROM payments;
