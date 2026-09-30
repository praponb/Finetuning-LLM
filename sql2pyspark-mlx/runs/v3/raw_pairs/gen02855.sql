select
    id,
    txn_time,
    merchant,
    case when merchant = 'Starbucks' then 1 else 0 end as is_starbucks
from transactions
order by merchant desc;
