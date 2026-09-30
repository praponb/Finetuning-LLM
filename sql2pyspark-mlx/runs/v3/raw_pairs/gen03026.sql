SELECT status, DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_date_label, TRIM(account_type) AS clean_account_type FROM accounts
