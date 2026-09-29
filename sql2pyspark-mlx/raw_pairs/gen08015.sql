select plan, sum(case when status = 'EXPIRED' then 1 else 0 end) as expired_count, round(avg(monthly_fee), 2) as avg_monthly_fee
from subscriptions
group by plan
having sum(case when status = 'EXPIRED' then 1 else 0 end) < 36
order by expired_count desc
