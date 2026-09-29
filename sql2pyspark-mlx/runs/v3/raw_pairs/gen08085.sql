select
    case when balance >= 67000 then 'high' when balance >= 33000 then 'mid' else 'low' end as balance_bucket,
    max(opened_date) as last_opened_date,
    concat_ws(', ', sort_array(collect_set(account_type))) as account_type_values,
    count(balance) as balance_count
from accounts
where balance between 50000 and 69000
group by 1
order by balance_count desc, balance_bucket
limit 50
