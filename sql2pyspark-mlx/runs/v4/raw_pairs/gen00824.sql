select
    merchant,
    account_id,
    avg(amount) as avg_amount,
    max(txn_time) as last_txn_time
from transactions
group by merchant, account_id
having avg(amount) >= 2300
order by last_txn_time, merchant, account_id
limit 10;
