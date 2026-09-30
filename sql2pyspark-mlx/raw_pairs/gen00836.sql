SELECT *, CONCAT(status, ' ', account_type) AS status_account_type
FROM accounts
WHERE customer_id IN (9, 17, 27, 35)
ORDER BY opened_date, customer_id DESC
