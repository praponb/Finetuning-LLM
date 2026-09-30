SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY opened_date) AS row_num FROM accounts) AS t WHERE row_num <= 1 ORDER BY id;
