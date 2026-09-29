select
    is_flagged,
    txn_time,
    amount,
    date_trunc('WEEK', txn_time) as txn_time_period
from transactions
where account_id in (5, 6, 14, 18) and account_id = 5
order by amount desc, txn_time_period asc
