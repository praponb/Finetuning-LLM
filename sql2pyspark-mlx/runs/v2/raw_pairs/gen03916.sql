select customer_id
from subscriptions
where monthly_fee <= 90
union
select customer_id
from subscriptions
where end_date between '2024-02-24' and '2025-10-09'
order by customer_id;
