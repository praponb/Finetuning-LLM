select
    start_date,
    monthly_fee,
    end_date,
    last_day(start_date) as start_date_month_end
from subscriptions
where monthly_fee >= 140
order by start_date, end_date
