SELECT id, account_type FROM accounts WHERE account_type != 'CHECKING' UNION ALL SELECT id, account_type FROM accounts WHERE MONTH(opened_date) IN (1, 3, 7);
