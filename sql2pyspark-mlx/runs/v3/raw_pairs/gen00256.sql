select id, account_id from transactions where txn_type = 'CREDIT' and merchant not in ('Amazon', '7-Eleven')
