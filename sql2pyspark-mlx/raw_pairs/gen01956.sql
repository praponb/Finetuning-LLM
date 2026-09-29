SELECT t.id, t.account_id
FROM transactions t
WHERE t.txn_type = 'DEBIT' AND t.account_id NOT IN (SELECT id FROM accounts WHERE balance > 54000)
