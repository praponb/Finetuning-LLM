select
    id,
    customer_id,
    plan,
    start_date,
    monthly_fee,
    status
from (
    select *, rank() over (partition by status order by end_date desc) as row_num
    from subscriptions
) t
where row_num = 1
