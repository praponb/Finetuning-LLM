with ranked as (
    select
        id,
        account_id,
        txn_time,
        txn_type,
        rank() over (partition by account_id order by txn_time desc) as pos
    from transactions
)
select *
from ranked
where pos <= 3
order by pos desc;
