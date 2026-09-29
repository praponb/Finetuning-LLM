SELECT id, customer_id, balance FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY balance DESC) AS rnk FROM accounts) AS r WHERE rnk <= 3
