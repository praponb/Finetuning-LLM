with ranked as (
    select *
    from (
        select *, row_number() over (partition by account_id, merchant order by amount, id) as latest_rank
        from transactions
    ) x
    where latest_rank = 1
)
select
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    is_flagged,
    latest_rank
from ranked;
