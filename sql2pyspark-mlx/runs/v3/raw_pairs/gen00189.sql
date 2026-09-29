select c.is_active, count(case when o.channel = 'web' then 1 end) as web_count, max(o.amount) - min(o.amount) as amount_range
from orders o
left join customers c on o.customer_id = c.id
group by c.is_active
order by web_count, is_active
limit 10;
