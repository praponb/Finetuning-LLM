select
    id,
    account_type,
    status,
    case when balance >= 62000 then 'High' when balance >= 25000 then 'Medium' else 'Low' end as balance_level
from accounts
where status in ('OPEN', 'CLOSED') or status <> 'CLOSED'
order by 1
