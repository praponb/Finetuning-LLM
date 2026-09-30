with filtered_accounts as (
    select *, case when status in ('FROZEN', 'OPEN') then 'Priority' else 'Other' end as status_group
    from accounts
    where status in ('CLOSED', 'OPEN')
),
summary as (
    select account_type, count(balance) as balance_count
    from filtered_accounts
    group by account_type
)
select *
from summary
where balance_count > 6
