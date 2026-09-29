select id, txn_type, account_id from transactions where is_flagged = false union select id, txn_type, account_id from transactions where account_id in (12, 37) order by account_id
