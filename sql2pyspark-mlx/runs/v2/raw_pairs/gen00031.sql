select s.monthly_fee, s.customer_id, c.country
from subscriptions s
join customers c on s.customer_id = c.id
where c.segment = 'SMB'
order by s.customer_id desc;
