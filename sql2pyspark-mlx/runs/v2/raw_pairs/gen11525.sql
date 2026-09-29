SELECT
    customer_id,
    balance,
    account_type,
    CEIL(balance) AS balance_whole,
    CONCAT_WS('-', account_type, status) AS account_type_status_key
FROM accounts
WHERE opened_date BETWEEN '2025-04-01' AND '2025-08-01'
