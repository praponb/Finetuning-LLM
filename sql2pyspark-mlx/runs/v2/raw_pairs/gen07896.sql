select
    date_format(opened_date, 'yyyy-MM') as opened_month,
    account_type,
    status,
    concat_ws(', ', sort_array(collect_set(status))) as status_values,
    sort_array(collect_set(account_type)) as account_type_list,
    min(opened_date) as min_opened_date
from accounts
where opened_date > '2024-07-01' and (status in ('OPEN', 'FROZEN') or account_type = 'SAVINGS')
group by 1, 2, 3
order by status_values;
