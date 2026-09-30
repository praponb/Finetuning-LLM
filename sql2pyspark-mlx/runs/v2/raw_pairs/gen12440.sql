select c.id, count(o.id) as order_count
from customers c
left outer join orders o on c.id = o.customer_id
group by c.id
