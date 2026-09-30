select id, status, balance
from (
    select
        id,
        status,
        balance,
        rank() over (partition by status order by balance) as pos
    from accounts
) as r
where pos <= 3
order by balance desc;
