select
    id,
    txn_type,
    txn_time,
    account_id
from (
    select
        id,
        txn_type,
        txn_time,
        account_id,
        dense_rank() over (partition by txn_type order by txn_time) as pos
    from transactions
) as ranked
where pos <= 2
order by id desc
