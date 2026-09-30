select
    id,
    merchant,
    txn_time,
    txn_type
from (
    select *, row_number() over (partition by merchant order by txn_time, id) as pos
    from transactions
) as r
where pos = 1
order by txn_time, id
