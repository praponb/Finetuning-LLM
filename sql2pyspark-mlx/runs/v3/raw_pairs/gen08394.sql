select c.segment, round(avg(s.monthly_fee), 2) avg_monthly_fee, max(s.start_date) latest_start_date
from subscriptions s
left outer join customers c on s.customer_id = c.id
where s.status = 'ACTIVE'
group by c.segment;
