select
    id,
    start_date,
    monthly_fee,
    end_date,
    datediff(end_date, start_date) / 7 as weeks_start_date_to_end_date
from subscriptions
order by weeks_start_date_to_end_date, end_date, id
limit 5;
