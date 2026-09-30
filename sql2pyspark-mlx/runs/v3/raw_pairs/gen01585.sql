SELECT account_type, opened_date FROM accounts WHERE balance BETWEEN 65000 AND 78000 UNION ALL SELECT account_type, opened_date FROM accounts WHERE opened_date < '2025-09-20'
