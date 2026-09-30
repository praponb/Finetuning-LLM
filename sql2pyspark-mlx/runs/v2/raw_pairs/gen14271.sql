select id, merchant, amount, txn_time from transactions where txn_type not in ('FEE', 'DEBIT') order by id desc, amount desc
