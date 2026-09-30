with Ranked as (
    select
        id,
        account_type,
        balance,
        status,
        row_number() over (partition by account_type order by balance desc, id) as pos
    from accounts
    where status in ('OPEN', 'CLOSED')
)
select
    id,
    account_type,
    balance,
    status
from Ranked
where pos <= 2;
