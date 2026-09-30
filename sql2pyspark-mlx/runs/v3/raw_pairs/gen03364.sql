select year(end_date) as end_year, count(distinct monthly_fee) as unique_statuss, round((100 * sum(case when plan = 'standard' then 1 else 0 end)) / count(*), 1) as pct_standard
from subscriptions
group by year(end_date)
order by unique_statuss
