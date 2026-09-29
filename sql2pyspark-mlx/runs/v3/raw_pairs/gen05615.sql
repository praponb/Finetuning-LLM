SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY opened_date DESC) AS pos FROM accounts) AS ranked WHERE pos <= 5;
