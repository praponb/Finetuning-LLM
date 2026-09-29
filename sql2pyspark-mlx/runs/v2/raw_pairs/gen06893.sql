SELECT accounts.id, accounts.customer_id, accounts.account_type
FROM accounts
WHERE NOT EXISTS (SELECT 1 FROM transactions WHERE transactions.account_id = accounts.id AND transactions.is_flagged)
ORDER BY accounts.customer_id;
