select plan, count(*) as total_count
from subscriptions
group by 1
order by total_count desc, plan
limit 5;
