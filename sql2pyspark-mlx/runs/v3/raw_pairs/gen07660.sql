SELECT id, customer_id, balance, opened_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) AS rn FROM accounts) AS r WHERE rn = 1 ORDER BY balance DESC;
