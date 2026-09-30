select
    status,
    max(balance) - min(balance) as balance_range,
    min(balance) as min_balance,
    concat_ws(', ', sort_array(collect_set(status))) as status_values
from accounts
where customer_id in (11, 21, 39)
group by status
