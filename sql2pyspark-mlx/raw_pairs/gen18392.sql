SELECT id, status, customer_id
FROM accounts
WHERE (balance >= 18000 AND YEAR(opened_date) >= 2025) OR account_type = 'CHECKING'
