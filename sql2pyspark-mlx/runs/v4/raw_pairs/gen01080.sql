select
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    dense_rank() over (partition by plan order by monthly_fee desc nulls last) monthly_fee_dense_rank,
    cume_dist() over (partition by customer_id order by monthly_fee desc nulls last) monthly_fee_cume_dist,
    sum(monthly_fee) over (order by monthly_fee, id rows between unbounded preceding and current row) cumulative_monthly_fee,
    ntile(10) over (partition by status, customer_id order by monthly_fee, id) monthly_fee_bucket,
    max(monthly_fee) over (partition by customer_id, status) customer_id_max_monthly_fee
from subscriptions
