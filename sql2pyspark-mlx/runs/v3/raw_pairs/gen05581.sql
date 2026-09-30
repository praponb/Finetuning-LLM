SELECT
    id,
    customer_id,
    account_type,
    balance
FROM accounts
WHERE status IN ('FROZEN', 'CLOSED', 'OPEN') OR balance BETWEEN 36000 AND 79000
ORDER BY 3;
