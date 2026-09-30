with filtered_accounts as (
    select *, datediff(current_date(), opened_date) as days_since_opened_date
    from accounts
    where account_type = 'CREDIT' and opened_date < '2025-06-16'
),
agg as (
    select date_trunc('MONTH', opened_date) as opened_month, sum(days_since_opened_date) as total_days_since_opened_date, sort_array(collect_set(account_type)) as account_type_list
    from filtered_accounts
    group by date_trunc('MONTH', opened_date)
)
select opened_month, total_days_since_opened_date, account_type_list
from agg
where total_days_since_opened_date < 350
order by total_days_since_opened_date desc, opened_month
limit 1;
