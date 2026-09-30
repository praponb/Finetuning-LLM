with ranked_transactions as (
    select
        id,
        account_id,
        txn_time,
        row_number() over (partition by account_id order by txn_time, id) as rnk
    from transactions
)
select id, account_id, txn_time
from ranked_transactions
where rnk <= 3
