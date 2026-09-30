SELECT id, account_type, balance, status FROM (SELECT id, account_type, balance, status, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance, id) AS rn FROM accounts) AS t WHERE rn <= 2;
