select
    id,
    txn_type,
    txn_time,
    is_flagged
from (
    select *, row_number() over (partition by txn_type order by txn_time desc, id) as rnk
    from transactions
    where account_id in (5, 25)
) as ranked
where rnk = 1;
