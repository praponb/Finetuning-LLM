SELECT
    id,
    account_type,
    customer_id,
    TRIM(status) AS clean_status,
    UPPER(account_type) AS account_type_upper
FROM accounts
ORDER BY clean_status DESC, customer_id DESC
