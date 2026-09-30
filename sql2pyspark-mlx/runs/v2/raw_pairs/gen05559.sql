SELECT status, method, MIN(paid_at) min_paid_at FROM payments GROUP BY 1, 2;
