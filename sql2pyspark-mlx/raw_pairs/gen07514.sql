select plan, round(avg(monthly_fee), 2) avg_monthly_fee, round((100 * sum(case when status = 'EXPIRED' then 1 else 0 end)) / count(*), 1) pct_expired
from subscriptions
group by plan
