select
    txn_type,
    merchant_group,
    min_txn_time,
    total_amount
from (
    select
        txn_type,
        case when merchant in ('Grab', 'Lazada') then 'priority' else 'other' end as merchant_group,
        min(txn_time) as min_txn_time,
        sum(amount) as total_amount
    from transactions
    group by txn_type, case when merchant in ('Grab', 'Lazada') then 'priority' else 'other' end
) as agg
where total_amount > 22000
