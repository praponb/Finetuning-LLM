SELECT id, customer_id, opened_date, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS rn FROM accounts ORDER BY 4 DESC;
