with top_transactions as (
    select *, row_number() over (partition by account_id order by txn_time desc, id) as pos
    from transactions
    where amount between 1700 and 3700
)
select id, account_id, txn_time
from top_transactions
where pos <= 3;
