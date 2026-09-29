select
    monthly_fee,
    start_date,
    plan,
    last_day(start_date) start_date_month_end,
    dayofweek(end_date) end_date_dayofweek
from subscriptions
where plan = 'standard' and end_date <= '2025-02-08'
