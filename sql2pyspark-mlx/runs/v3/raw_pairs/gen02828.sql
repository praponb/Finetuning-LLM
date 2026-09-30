SELECT account_type, LENGTH(status) AS status_length, DATE_SUB(opened_date, 83) AS opened_date_minus_83d FROM accounts WHERE account_type <> 'CREDIT'
