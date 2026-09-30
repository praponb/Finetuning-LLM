SELECT
    accounts.id,
    accounts.status,
    accounts.balance,
    accounts.account_type
FROM accounts
WHERE accounts.status IS NULL AND EXISTS (SELECT 1 FROM transactions WHERE transactions.account_id = accounts.id)
