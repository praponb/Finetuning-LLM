SELECT account_type, opened_date, DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label
FROM accounts
WHERE balance <= 40000 AND customer_id IS NULL
