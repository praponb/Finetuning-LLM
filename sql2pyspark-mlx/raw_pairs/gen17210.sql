SELECT account_type, COUNT(*) total_count FROM accounts WHERE account_type = 'CHECKING' GROUP BY account_type HAVING total_count > 5;
