select
    id,
    account_id,
    txn_time,
    merchant,
    quarter(txn_time) as txn_time_quarter
from transactions
where amount >= 3100 and amount < 3300
order by merchant desc;
