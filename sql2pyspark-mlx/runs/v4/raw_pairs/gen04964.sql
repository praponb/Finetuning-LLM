SELECT t.id, t.account_id, t.merchant FROM transactions t WHERE TO_DATE(t.txn_time) < '2024-08-01' AND t.account_id IN (SELECT id FROM accounts WHERE balance > 26000)
