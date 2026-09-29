with ranked_transactions as (
    select *, row_number() over (partition by merchant order by txn_time desc, id) as rnk
    from transactions
)
select *
from ranked_transactions
where rnk <= 5
