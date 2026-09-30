SELECT t.id, t.account_id, t.amount, t.txn_type FROM transactions t WHERE t.amount < (SELECT AVG(t2.amount) FROM transactions t2 WHERE t2.account_id = t.account_id);
