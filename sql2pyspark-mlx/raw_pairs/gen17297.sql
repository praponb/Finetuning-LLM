select id, txn_time, amount, sum(amount) over (order by txn_time, id) as running_amount from transactions where txn_type in ('CREDIT', 'DEBIT')
