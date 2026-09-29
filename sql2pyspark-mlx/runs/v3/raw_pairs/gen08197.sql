SELECT id, account_type, ADD_MONTHS(opened_date, 1) AS opened_date_plus_months
FROM accounts
WHERE customer_id = 5 AND (customer_id IS NOT NULL OR account_type <> 'LOAN')
ORDER BY account_type
