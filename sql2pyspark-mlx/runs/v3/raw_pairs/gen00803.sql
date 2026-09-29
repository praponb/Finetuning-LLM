SELECT id, account_id, amount FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY account_id ORDER BY amount) AS rn FROM transactions) AS ranked WHERE rn <= 1
