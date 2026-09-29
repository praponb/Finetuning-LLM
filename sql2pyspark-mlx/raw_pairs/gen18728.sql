select c.segment, count(case when s.status = 'CANCELLED' then 1 end) as cancelled_count
from subscriptions s
left outer join customers c on s.customer_id = c.id
where s.customer_id is not null
group by c.segment
