SELECT id, balance, status, CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END AS is_open, ADD_MONTHS(opened_date, 6) AS opened_date_plus_months FROM accounts
