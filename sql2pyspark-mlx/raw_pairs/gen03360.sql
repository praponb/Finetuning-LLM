select c.is_active, sum(s.monthly_fee) total_monthly_fee, round(avg(s.monthly_fee), 2) avg_monthly_fee
from subscriptions s
join customers c on s.customer_id = c.id
group by c.is_active
order by avg_monthly_fee desc;
