select
    status,
    date_trunc('year', opened_date) as year_start,
    sort_array(collect_set(status)) as status_list,
    min(balance) as min_balance,
    grouping_id() as grouping_level
from accounts
group by grouping sets ((status, date_trunc('year', opened_date)), (status))
order by min_balance;
