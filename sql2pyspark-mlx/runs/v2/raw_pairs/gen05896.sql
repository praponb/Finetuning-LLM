SELECT *, ADD_MONTHS(opened_date, 1) AS opened_date_plus_months, ADD_MONTHS(opened_date, 12) AS opened_date_plus_months_2
FROM accounts
ORDER BY account_type
