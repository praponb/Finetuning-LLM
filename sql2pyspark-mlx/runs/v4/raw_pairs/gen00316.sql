select id, merchant, txn_type, txn_time, amount, lead(amount, 1) over (partition by merchant, txn_type order by txn_time, id) as next_amount from transactions
