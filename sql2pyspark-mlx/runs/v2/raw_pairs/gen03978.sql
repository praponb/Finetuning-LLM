SELECT t.id, t.account_id, t.merchant FROM transactions t WHERE t.account_id IN (SELECT id FROM accounts WHERE balance IS NULL) ORDER BY t.account_id DESC;
