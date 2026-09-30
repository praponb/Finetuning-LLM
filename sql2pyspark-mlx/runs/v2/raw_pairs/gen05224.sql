with top_transactions as (
    select
        id,
        merchant,
        amount,
        dense_rank() over (partition by merchant order by amount) as rn
    from transactions
    where account_id in (5, 37)
)
select *
from top_transactions
where rn <= 2
order by rn
