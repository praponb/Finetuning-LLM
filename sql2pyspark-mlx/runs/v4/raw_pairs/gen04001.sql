with src as (
    select *
    from subscriptions
    where monthly_fee < 110 and plan = 'enterprise'
),
subscriptions_stats as (
    select date_format(end_date, 'yyyy-MM') as end_month, year(start_date) as start_year, count(*) as total_count
    from src
    group by date_format(end_date, 'yyyy-MM'), year(start_date)
)
select *
from subscriptions_stats;
