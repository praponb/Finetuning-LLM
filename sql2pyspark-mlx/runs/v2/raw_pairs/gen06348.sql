SELECT status, account_type, DATE_FORMAT(opened_date, 'yyyy-MM-dd') AS opened_date_label
FROM accounts
ORDER BY account_type
