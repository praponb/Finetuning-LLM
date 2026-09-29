select id, is_flagged from transactions where txn_type = 'DEBIT' order by id limit 20;
