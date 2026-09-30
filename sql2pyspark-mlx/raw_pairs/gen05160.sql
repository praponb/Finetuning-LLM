SELECT id, customer_id, CONCAT(account_type, ' ', status) AS account_type_status
FROM accounts
