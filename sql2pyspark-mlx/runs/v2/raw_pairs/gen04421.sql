SELECT * FROM (SELECT id, status, balance, opened_date, RANK() OVER (PARTITION BY status ORDER BY balance) AS pos FROM accounts) AS r WHERE pos <= 3 ORDER BY id
