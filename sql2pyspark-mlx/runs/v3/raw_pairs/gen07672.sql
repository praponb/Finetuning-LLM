SELECT
    accounts.id,
    accounts.customer_id,
    accounts.status,
    accounts.balance
FROM accounts
WHERE accounts.status NOT IN ('FROZEN', 'CLOSED', 'OPEN') AND NOT EXISTS (SELECT 1 FROM transactions WHERE transactions.account_id = accounts.id)
ORDER BY balance
