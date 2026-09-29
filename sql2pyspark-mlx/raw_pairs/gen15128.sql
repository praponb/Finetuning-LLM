with base as (
    select *
    from accounts
    where account_type in ('LOAN', 'CHECKING')
),
AccountsStats as (
    select status, case when balance >= 70000 then 'high' when balance >= 7000 then 'mid' else 'low' end as balance_bucket, min(balance) as min_balance
    from base
    group by status, case when balance >= 70000 then 'high' when balance >= 7000 then 'mid' else 'low' end
)
select status, balance_bucket, min_balance
from AccountsStats
order by balance_bucket desc
