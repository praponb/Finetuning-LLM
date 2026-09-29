SELECT id, customer_id, balance, RANK() OVER (PARTITION BY customer_id ORDER BY balance DESC) AS rnk FROM accounts;
