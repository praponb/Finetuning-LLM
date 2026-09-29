SELECT
    id,
    balance,
    opened_date,
    account_type
FROM accounts
WHERE customer_id = 37
ORDER BY account_type, id DESC
