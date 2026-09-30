select status, count(monthly_fee) as monthly_fee_count, round((100 * sum(case when plan = 'standard' then 1 else 0 end)) / count(*), 1) as pct_standard
from subscriptions
group by status
order by pct_standard, status
limit 20
