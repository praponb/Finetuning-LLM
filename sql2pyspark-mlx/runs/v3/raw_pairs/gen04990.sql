SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY status ORDER BY balance DESC) AS rn FROM accounts) AS ranked WHERE rn <= 2 ORDER BY id DESC
