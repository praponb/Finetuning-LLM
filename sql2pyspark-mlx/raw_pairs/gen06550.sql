SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance, id) AS row_num FROM accounts WHERE customer_id IS NOT NULL) AS r WHERE row_num <= 2
