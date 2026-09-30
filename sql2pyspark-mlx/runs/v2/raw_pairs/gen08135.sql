with ranked_transactions as (
    select *, row_number() over (partition by merchant order by amount desc, id) as pos
    from transactions
    where to_date(txn_time) >= '2025-05-01'
)
select
    id,
    merchant,
    amount,
    txn_time
from ranked_transactions
where pos <= 3
