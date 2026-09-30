SELECT
    id,
    balance,
    status,
    customer_id
FROM accounts
WHERE customer_id IS NULL;
