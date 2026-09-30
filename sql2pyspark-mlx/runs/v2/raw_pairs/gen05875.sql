SELECT t.id, t.txn_type, t.merchant
FROM transactions t
WHERE t.account_id IN (SELECT id FROM accounts WHERE balance >= 22000);
