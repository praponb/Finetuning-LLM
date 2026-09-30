SELECT t.id, t.is_flagged, t.txn_time FROM transactions t WHERE t.txn_type = 'REFUND' AND t.account_id IN (SELECT id FROM accounts WHERE status != 'CLOSED');
