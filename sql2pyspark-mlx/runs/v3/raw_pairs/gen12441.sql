select max(amount) - min(amount) as amount_range from transactions where txn_type not in ('DEBIT', 'REFUND') order by amount_range desc
