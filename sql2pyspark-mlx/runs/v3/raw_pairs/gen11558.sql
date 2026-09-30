SELECT id, customer_id, balance, account_type FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) AS rnk FROM accounts) AS ranked WHERE rnk <= 3;
