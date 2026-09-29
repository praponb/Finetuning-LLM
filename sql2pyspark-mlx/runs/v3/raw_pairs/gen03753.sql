SELECT * FROM (SELECT id, customer_id, opened_date, status, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date DESC, id) AS rnk FROM accounts) AS ranked WHERE rnk = 1;
