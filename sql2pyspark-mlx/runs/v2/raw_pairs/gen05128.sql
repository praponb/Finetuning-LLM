WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) AS rn FROM accounts) SELECT id, customer_id, balance FROM numbered WHERE rn = 1;
