select s.id, s.start_date
from subscriptions s
where s.customer_id not in (select id from customers where country in ('US', 'JP'))
order by s.id desc, s.start_date
limit 5
