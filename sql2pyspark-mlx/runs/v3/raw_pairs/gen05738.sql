SELECT t.id, t.account_id, t.txn_type
FROM transactions t
WHERE t.amount < 1100 AND t.account_id NOT IN (SELECT id FROM accounts WHERE customer_id IS NOT NULL)
ORDER BY t.account_id DESC, id
LIMIT 5
