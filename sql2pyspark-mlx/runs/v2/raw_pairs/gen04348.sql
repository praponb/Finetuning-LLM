SELECT t.id, t.account_id, t.amount, t.merchant FROM transactions t WHERE t.amount = (SELECT MIN(t2.amount) FROM transactions t2 WHERE t2.account_id = t.account_id);
