SELECT s.id, s.account_id, s.txn_time, s.merchant, s.is_flagged, p.account_type, p.balance, p.opened_date FROM transactions s INNER JOIN accounts p ON s.account_id = p.id WHERE p.balance < 16000
