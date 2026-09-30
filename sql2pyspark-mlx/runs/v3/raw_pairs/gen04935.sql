WITH ranked AS (SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY balance DESC) AS pos FROM accounts) SELECT * FROM ranked WHERE pos <= 3 ORDER BY customer_id DESC, balance DESC;
