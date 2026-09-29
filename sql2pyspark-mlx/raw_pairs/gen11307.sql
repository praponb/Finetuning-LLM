select id, is_flagged, txn_type from transactions where (account_id in (6, 18) and amount <= 3400) or txn_type = 'REFUND'
