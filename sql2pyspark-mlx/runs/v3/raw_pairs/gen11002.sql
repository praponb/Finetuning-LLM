select
    id,
    txn_type,
    amount,
    txn_time
from (
    select *, rank() over (partition by txn_type order by amount desc) as row_num
    from transactions
    where txn_type in ('FEE', 'CREDIT')
) as ranked
where row_num <= 3
order by txn_type
