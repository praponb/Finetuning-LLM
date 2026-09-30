SELECT id, account_type, balance, status FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY account_type ORDER BY balance DESC) AS rn FROM accounts WHERE balance <= 29000) AS r WHERE rn = 1;
