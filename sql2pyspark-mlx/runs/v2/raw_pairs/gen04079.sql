select
    id,
    status,
    plan,
    end_date,
    monthly_fee,
    start_date,
    sum(monthly_fee) over (partition by status, plan order by end_date, id rows between unbounded preceding and current row) as running_monthly_fee,
    row_number() over (partition by status order by start_date, id) as rn
from subscriptions
where customer_id in (22, 29, 30, 32)
order by id desc;
