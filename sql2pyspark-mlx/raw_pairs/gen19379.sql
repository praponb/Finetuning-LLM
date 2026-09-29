select id, txn_time, merchant, amount, lag(amount, 2) over (partition by month(txn_time), merchant order by txn_time, id) as prev_amount from transactions
