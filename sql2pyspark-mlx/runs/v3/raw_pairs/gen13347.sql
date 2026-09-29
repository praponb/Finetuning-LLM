select c.country, sum(s.monthly_fee) as total_monthly_fee, round(avg(s.monthly_fee), 2) as avg_monthly_fee
from subscriptions s
left outer join customers c on s.customer_id = c.id
group by c.country
having sum(s.monthly_fee) >= 580
order by c.country
limit 50;
