SELECT customer_id, ADD_MONTHS(opened_date, 6) AS opened_date_plus_months
FROM accounts
WHERE account_type IN ('LOAN', 'CREDIT', 'SAVINGS') OR account_type IN ('CHECKING', 'CREDIT', 'SAVINGS')
