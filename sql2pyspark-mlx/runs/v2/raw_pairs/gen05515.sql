SELECT account_type, MIN(opened_date) AS first_opened_date FROM accounts GROUP BY account_type ORDER BY account_type DESC LIMIT 3
