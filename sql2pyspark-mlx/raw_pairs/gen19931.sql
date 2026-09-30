SELECT * FROM (SELECT id, status, opened_date, customer_id, RANK() OVER (PARTITION BY status ORDER BY opened_date DESC) AS pos FROM accounts) AS ranked WHERE pos <= 3 ORDER BY customer_id;
