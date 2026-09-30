WITH ranked AS (SELECT id, status, balance, DENSE_RANK() OVER (PARTITION BY status ORDER BY balance DESC) AS rnk FROM accounts) SELECT id, status, balance FROM ranked WHERE rnk <= 3;
