SELECT accounts.id, accounts.account_type, accounts.opened_date
FROM accounts
WHERE accounts.id IN (SELECT account_id FROM transactions WHERE txn_type IN ('FEE', 'CREDIT', 'DEBIT'))
ORDER BY id DESC, accounts.account_type DESC;
