select
    customer_id,
    stddev_samp(balance) as balance_stddev,
    round(avg(balance), 2) as avg_balance,
    max(balance) filter (where status = 'FROZEN') as frozen_max_balance
from accounts
group by customer_id
