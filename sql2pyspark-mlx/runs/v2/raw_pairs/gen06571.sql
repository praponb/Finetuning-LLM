select
    id,
    customer_id,
    end_date,
    start_date,
    monthly_fee,
    row_number() over (partition by customer_id order by end_date desc, id) as row_num,
    round(avg(monthly_fee) over (partition by customer_id order by start_date, id rows between 6 preceding and current row), 2) as monthly_fee_moving_avg_7
from subscriptions
