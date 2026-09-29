WITH ranked AS (SELECT *, RANK() OVER (PARTITION BY account_id ORDER BY amount DESC) AS rn FROM transactions) SELECT id, account_id, amount FROM ranked WHERE rn <= 3 ORDER BY id DESC;
