with filtered_accounts as (
    select *
    from accounts
    where (account_type = 'CREDIT' and status = 'OPEN') or status in ('OPEN', 'CLOSED', 'FROZEN')
),
Summary as (
    select
        status,
        account_type,
        max(opened_date) as latest_opened_date,
        sort_array(collect_set(account_type)) as account_type_list
    from filtered_accounts
    group by status, account_type
)
select *
from Summary
order by account_type
