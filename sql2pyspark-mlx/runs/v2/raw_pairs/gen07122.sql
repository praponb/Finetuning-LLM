SELECT id, account_type, CAST(balance AS INT) AS balance_int
FROM accounts
WHERE NOT (opened_date >= '2024-03-12' AND balance BETWEEN 51000 AND 79000);
