select
    is_flagged,
    account_id,
    min(txn_time) as first_txn_time,
    min(txn_time) as min_txn_time
from transactions
where account_id in (8, 37) and amount >= 1600
group by is_flagged, account_id;
