select s.id, s.start_date, c.country
from subscriptions s
join customers c on c.id = s.customer_id
where s.customer_id is null and s.status in ('CANCELLED', 'EXPIRED')
order by start_date desc;
