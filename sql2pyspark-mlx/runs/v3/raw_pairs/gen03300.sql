SELECT account_type, COUNT(DISTINCT balance) AS unique_account_types FROM accounts GROUP BY account_type ORDER BY unique_account_types ASC, account_type DESC
