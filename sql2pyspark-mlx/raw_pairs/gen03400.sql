select
    id,
    plan,
    start_date,
    monthly_fee,
    status,
    row_number() over (partition by plan order by monthly_fee, id) as event_seq,
    avg(monthly_fee) over (order by monthly_fee, id rows between 5 preceding and 1 preceding) as monthly_fee_avg_5_prior,
    ntile(3) over (partition by customer_id order by monthly_fee, id) as monthly_fee_bucket,
    max(monthly_fee) over (partition by status, plan) as status_max_monthly_fee,
    last_value(monthly_fee) ignore nulls over (partition by customer_id order by monthly_fee, id rows between unbounded preceding and current row) as last_known_monthly_fee
from subscriptions;
