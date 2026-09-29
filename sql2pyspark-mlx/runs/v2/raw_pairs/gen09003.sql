with top_transactions as (
    select
        id,
        account_id,
        amount,
        txn_time,
        row_number() over (partition by account_id order by amount, id) as rnk
    from transactions
    where account_id in (7, 31, 35)
)
select
    id,
    account_id,
    amount,
    txn_time
from top_transactions
where rnk = 1
order by id desc;
