SELECT account_type, ROUND(balance * 100, 2) AS balance_adjusted FROM accounts WHERE account_type IN ('LOAN', 'SAVINGS', 'CREDIT') OR balance > 65000 ORDER BY balance_adjusted ASC
