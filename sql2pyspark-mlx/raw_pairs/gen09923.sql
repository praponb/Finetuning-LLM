SELECT id, customer_id, CONCAT_WS('-', account_type, status) account_type_status_key
FROM accounts
WHERE customer_id = 39 AND opened_date < '2024-02-01'
ORDER BY account_type_status_key;
