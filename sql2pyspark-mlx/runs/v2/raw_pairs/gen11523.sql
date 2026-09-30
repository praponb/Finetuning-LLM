with recent_accounts as (
    select *
    from accounts
    where account_type in ('CREDIT', 'SAVINGS', 'CHECKING') and account_type not in ('LOAN', 'CHECKING')
),
agg as (
    select status, min(opened_date) as first_opened_date
    from recent_accounts
    group by status
)
select status, first_opened_date
from agg
order by first_opened_date desc, status desc
