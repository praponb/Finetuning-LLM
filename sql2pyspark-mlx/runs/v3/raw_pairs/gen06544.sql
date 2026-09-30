select c.segment, count(o.id) as order_count, sum(o.discount) as total_discount
from customers c
join orders o on c.id = o.customer_id
group by c.segment;
