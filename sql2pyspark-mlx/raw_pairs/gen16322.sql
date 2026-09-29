SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance, id) AS rnk FROM accounts WHERE customer_id IS NULL) AS r WHERE rnk <= 5 ORDER BY account_type, customer_id DESC
