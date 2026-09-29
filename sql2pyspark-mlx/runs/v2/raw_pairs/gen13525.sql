SELECT
    id,
    customer_id,
    balance,
    account_type,
    CONCAT(status, ' - ', account_type) AS status_account_type
FROM accounts
WHERE customer_id IS NOT NULL OR account_type = 'LOAN'
ORDER BY status_account_type DESC, id ASC
LIMIT 20
