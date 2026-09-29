SELECT id, account_type, opened_date, balance FROM (SELECT *, RANK() OVER (PARTITION BY account_type ORDER BY opened_date DESC) AS rn FROM accounts) AS ranked WHERE rn = 1
