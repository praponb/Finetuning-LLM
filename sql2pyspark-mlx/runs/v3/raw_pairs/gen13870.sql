SELECT t.id, t.is_flagged, t.merchant
FROM transactions t
WHERE t.txn_type = 'CREDIT' AND t.account_id NOT IN (SELECT id FROM accounts WHERE balance BETWEEN 67000 AND 76000)
