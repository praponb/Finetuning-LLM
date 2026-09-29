select o.order_date, c.name, s.store_name
from orders o
join customers c on c.id = o.customer_id
left join stores s on o.store_id = s.id
