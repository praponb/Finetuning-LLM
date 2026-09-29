select
    date_trunc('MONTH', start_date) as start_month,
    sum(case when plan = 'premium' then monthly_fee else 0 end) as premium_monthly_fee,
    min(end_date) as first_end_date,
    max(end_date) as last_end_date
from subscriptions
where month(start_date) in (2, 8, 12)
group by date_trunc('MONTH', start_date)
having sum(case when plan = 'premium' then monthly_fee else 0 end) > 75;
