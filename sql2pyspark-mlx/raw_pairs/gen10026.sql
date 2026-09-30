select
    customer_id,
    sort_array(collect_set(account_type)) as account_type_list,
    size(collect_set(status)) as num_status,
    stddev_samp(balance) as balance_stddev,
    count(*) as row_count,
    sort_array(collect_set(account_type)) as account_type_list_2,
    avg(balance) filter (where status = 'FROZEN') as frozen_avg_balance
from accounts
group by customer_id
having frozen_avg_balance < 1000000
order by num_status;
