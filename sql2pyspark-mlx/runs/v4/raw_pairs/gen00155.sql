SELECT id, method, paid_at, DENSE_RANK() OVER (PARTITION BY method ORDER BY paid_at) AS dense_rnk FROM payments WHERE method = 'cash'
