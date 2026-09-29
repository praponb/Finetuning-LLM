SELECT method, MIN(paid_at) min_paid_at, MAX(amount) max_amount FROM payments GROUP BY method ORDER BY max_amount DESC, method LIMIT 50;
