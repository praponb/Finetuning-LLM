select
    account_type,
    status,
    concat_ws(', ', sort_array(collect_set(account_type))) as account_type_values,
    min(opened_date) as min_opened_date,
    count(*) as account_count
from accounts
where status = 'FROZEN'
group by account_type, status
having count(*) >= 4
order by account_type
