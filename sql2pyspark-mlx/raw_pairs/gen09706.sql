select
    merchant,
    txn_type,
    count(*) as cnt,
    round(stddev(amount), 2) as stddev_amount
from transactions
group by 1, 2;
