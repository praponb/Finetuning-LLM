select c.segment, count(o.id) as order_count
from customers c
left join orders o on o.customer_id = c.id
group by c.segment
order by c.segment
limit 3;
