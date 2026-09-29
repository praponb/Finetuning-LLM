SELECT t.id, t.is_flagged, t.merchant FROM transactions t WHERE t.txn_time < '2023-03-26' AND t.account_id NOT IN (SELECT id FROM accounts WHERE account_type IN ('LOAN', 'SAVINGS'))
