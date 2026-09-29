SELECT
    accounts.id,
    accounts.opened_date,
    accounts.account_type,
    accounts.status
FROM accounts
WHERE accounts.balance IS NULL AND EXISTS (SELECT 1 FROM transactions WHERE transactions.account_id = accounts.id AND transactions.merchant = 'Amazon')
