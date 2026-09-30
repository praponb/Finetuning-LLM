WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC, id) AS rn FROM accounts) SELECT id, status, balance, opened_date FROM ranked WHERE rn <= 3;
