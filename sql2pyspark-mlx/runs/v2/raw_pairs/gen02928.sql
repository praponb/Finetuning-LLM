SELECT id, balance, opened_date, DATE_SUB(opened_date, 10) opened_date_minus_10d FROM accounts ORDER BY opened_date;
