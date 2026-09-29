select is_flagged, coalesce(amount, 0) as amount_filled from transactions where (account_id in (10, 16, 32) and hour(txn_time) between 11 and 14) or txn_type = 'DEBIT' order by amount_filled;
