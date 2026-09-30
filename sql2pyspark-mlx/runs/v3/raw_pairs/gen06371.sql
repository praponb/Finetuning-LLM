select id, merchant, amount, txn_type from transactions where txn_type in ('FEE', 'DEBIT')
