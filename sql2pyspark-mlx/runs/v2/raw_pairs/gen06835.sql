select id, txn_type, txn_time
from (
    select
        id,
        txn_type,
        txn_time,
        rank() over (partition by txn_type order by txn_time desc) rn
    from transactions
) r
where rn = 1;
