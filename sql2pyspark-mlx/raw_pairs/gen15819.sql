select status, count(monthly_fee) as monthly_fee_count, max(monthly_fee) - min(monthly_fee) as monthly_fee_range
from subscriptions
where plan in ('basic', 'standard')
group by 1
order by monthly_fee_count desc
