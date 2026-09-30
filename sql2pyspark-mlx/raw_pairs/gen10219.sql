select
    status,
    year(start_date) start_year,
    round((100 * sum(case when plan = 'enterprise' then 1 else 0 end)) / count(*), 1) pct_enterprise,
    min(start_date) first_start_date
from subscriptions
where start_date <= '2024-02-08' and customer_id is not null and status = 'PAUSED'
group by 1, 2;
