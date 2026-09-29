select customer_id
from subscriptions
where plan in ('basic', 'enterprise')
union
select customer_id
from subscriptions
where end_date between '2024-03-01' and '2024-07-01';
