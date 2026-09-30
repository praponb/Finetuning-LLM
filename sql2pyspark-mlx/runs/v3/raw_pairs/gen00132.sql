with numbered as (
    select
        id,
        txn_type,
        txn_time,
        account_id,
        row_number() over (partition by txn_type order by txn_time, id) as row_num
    from transactions
)
select
    id,
    txn_type,
    txn_time,
    account_id
from numbered
where row_num <= 3
order by id;
