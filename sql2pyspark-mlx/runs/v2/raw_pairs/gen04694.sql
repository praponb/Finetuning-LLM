SELECT t.id, t.is_flagged FROM transactions t WHERE t.account_id IN (2, 14, 20) AND t.account_id NOT IN (SELECT id FROM accounts WHERE balance >= 61000);
