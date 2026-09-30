SELECT id, account_type, opened_date, LEAD(opened_date, 2) OVER (PARTITION BY account_type ORDER BY opened_date, id) AS next_opened_date FROM accounts ORDER BY opened_date;
