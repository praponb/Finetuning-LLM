SELECT customer_id, account_type, LENGTH(account_type) AS account_type_length, CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END AS is_open FROM accounts WHERE status = 'OPEN' ORDER BY account_type
