SELECT id, status, amount, paid_at FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount, id) rnk FROM payments) t WHERE rnk = 1
