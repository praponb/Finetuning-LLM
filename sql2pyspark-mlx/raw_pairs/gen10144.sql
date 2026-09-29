select
    id,
    opened_date,
    case when balance >= 71000 then 'High' when balance >= 9000 then 'Medium' else 'Low' end as balance_tier,
    substring(account_type, 1, 3) as account_type_prefix
from accounts
where status = 'CLOSED' and balance > 40000 and status in ('CLOSED', 'FROZEN')
