SELECT status, MIN(paid_at) min_paid_at, AVG(amount) avg_amount FROM payments GROUP BY status ORDER BY avg_amount, status LIMIT 20;
