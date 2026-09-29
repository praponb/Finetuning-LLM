select c.is_active, c.country, sum(s.monthly_fee) as total_monthly_fee
from subscriptions s
inner join customers c on s.customer_id = c.id
group by c.is_active, c.country
having sum(s.monthly_fee) >= 1360;
