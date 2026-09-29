SELECT account_type, COUNT(*) total_count
FROM accounts
WHERE account_type = 'SAVINGS' OR status != 'FROZEN'
GROUP BY account_type;
