SELECT status, method, ROUND(AVG(amount), 2) avg_amount, MAX(amount) - MIN(amount) amount_range FROM payments WHERE method IS NOT NULL GROUP BY status, method HAVING amount_range < 76
