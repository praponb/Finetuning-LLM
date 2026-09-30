with valid_transactions as (
    select *
    from transactions
    where hour(txn_time) between 6 and 14
),
summary as (
    select merchant, count(distinct amount) as unique_accounts
    from valid_transactions
    group by merchant
)
select merchant, unique_accounts
from summary
where unique_accounts > 8
order by merchant asc
limit 3;
