WITH ranked_payments AS (SELECT *, DENSE_RANK() OVER (PARTITION BY method ORDER BY paid_at DESC) pos FROM payments) SELECT id, method, paid_at, status FROM ranked_payments WHERE pos <= 2
