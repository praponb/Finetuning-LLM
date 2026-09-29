SELECT id, customer_id, balance FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) AS pos FROM accounts) AS r WHERE pos <= 3 ORDER BY customer_id DESC
