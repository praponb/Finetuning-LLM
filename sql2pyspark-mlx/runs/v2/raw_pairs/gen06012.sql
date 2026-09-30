select id, monthly_fee, ntile(2) over (order by monthly_fee, id) as bucket
from subscriptions
where customer_id in (4, 12, 14);
