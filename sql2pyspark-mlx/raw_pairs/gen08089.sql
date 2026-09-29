SELECT id, customer_id, opened_date, status FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY opened_date) AS rnk FROM accounts WHERE customer_id = 40) AS ranked WHERE rnk <= 3;
