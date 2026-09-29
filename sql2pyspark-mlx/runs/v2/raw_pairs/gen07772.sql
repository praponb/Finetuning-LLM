select
    id,
    account_type,
    balance,
    status
from (
    select *, rank() over (partition by account_type order by balance desc) pos
    from accounts
    where balance < 66000
) t
where pos <= 3
order by status desc;
