with base as (
    select *, substring(status, 1, 4) as status_prefix
    from accounts
    where account_type in ('CHECKING', 'SAVINGS')
),
summary as (
    select status, min(opened_date) as min_opened_date, count(*) as total_count
    from base
    group by status
)
select *
from summary
where total_count >= 7
