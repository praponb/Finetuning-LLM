select monthly_fee, customer_id, lower(plan) as plan_lower
from subscriptions
where monthly_fee <= 70 or customer_id in (11, 15, 29, 32)
order by monthly_fee desc
