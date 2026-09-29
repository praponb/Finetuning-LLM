SELECT * FROM (SELECT id, method, paid_at, amount, ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at DESC, id) AS rnk FROM payments) AS t WHERE rnk <= 3 ORDER BY amount
