with ranked_transactions as (
    select *, row_number() over (partition by account_id order by amount desc, id) as pos
    from transactions
)
select *
from ranked_transactions
where pos = 1
order by txn_time, txn_type
