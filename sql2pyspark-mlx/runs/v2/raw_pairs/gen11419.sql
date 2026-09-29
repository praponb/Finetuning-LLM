select plan, count(distinct monthly_fee) as unique_plans
from subscriptions
group by plan
