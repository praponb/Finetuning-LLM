SELECT t.id, t.txn_time, t.merchant, t.is_flagged FROM transactions t WHERE t.account_id IN (SELECT id FROM accounts WHERE opened_date BETWEEN '2023-11-18' AND '2024-12-01')
