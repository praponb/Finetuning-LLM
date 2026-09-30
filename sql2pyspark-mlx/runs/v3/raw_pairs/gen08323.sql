SELECT id, opened_date, CONCAT_WS('-', account_type, status) AS account_type_status_key
FROM accounts
WHERE opened_date <= '2023-10-01' AND customer_id IS NULL
ORDER BY opened_date, id;
