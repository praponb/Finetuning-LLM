SELECT id, status, balance, RANK() OVER (PARTITION BY status ORDER BY balance DESC) AS balance_rank FROM accounts WHERE balance BETWEEN 24000 AND 52000
