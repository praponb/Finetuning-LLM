select *
from (
    select *, row_number() over (partition by account_id order by amount desc, id) pos
    from transactions
    where txn_time >= '2024-01-01'
) r
where pos <= 3
order by amount desc
