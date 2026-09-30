SELECT
    id,
    balance,
    account_type,
    status
FROM accounts
WHERE customer_id IN (8, 35)
