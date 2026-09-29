SELECT
    accounts.id,
    accounts.balance,
    accounts.opened_date,
    accounts.account_type
FROM accounts
WHERE accounts.customer_id IN (2, 23, 38) AND NOT EXISTS (SELECT 1 FROM transactions WHERE transactions.account_id = accounts.id AND transactions.is_flagged)
