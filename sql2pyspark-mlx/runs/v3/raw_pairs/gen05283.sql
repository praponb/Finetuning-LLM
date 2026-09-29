with ranked_transactions as (
    select
        id,
        account_id,
        amount,
        is_flagged,
        dense_rank() over (partition by account_id order by amount) as pos
    from transactions
)
select
    id,
    account_id,
    amount,
    is_flagged
from ranked_transactions
where pos <= 3
order by 2 desc;
