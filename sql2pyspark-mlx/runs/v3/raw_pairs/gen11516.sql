select
    id,
    plan,
    end_date,
    monthly_fee,
    lead(monthly_fee, 2) over (partition by plan order by end_date, id) as next_monthly_fee
from subscriptions
order by end_date
