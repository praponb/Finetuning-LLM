select o.discount, s.city, c.city as customer_city
from orders o
inner join stores s on o.store_id = s.id
inner join customers c on o.customer_id = c.id
order by s.city desc
