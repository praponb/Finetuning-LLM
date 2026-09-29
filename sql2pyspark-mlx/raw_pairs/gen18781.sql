SELECT SUM(amount) total_amount, MIN(paid_at) min_paid_at, MIN(amount) min_amount FROM payments ORDER BY min_amount, 2
