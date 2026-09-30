SELECT account_type, status, MIN(opened_date) AS first_opened_date FROM accounts WHERE balance < 42000 GROUP BY account_type, status;
