SELECT t.id, t.txn_time, t.is_flagged FROM transactions t WHERE t.account_id NOT IN (SELECT id FROM accounts WHERE YEAR(opened_date) = 2023) ORDER BY t.id DESC;
