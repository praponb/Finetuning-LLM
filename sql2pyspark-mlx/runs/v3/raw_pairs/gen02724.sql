select p.status, o.discount, c.age
from payments p
inner join orders o on p.order_id = o.id
inner join customers c on o.customer_id = c.id
where c.country = 'US' and c.city in ('Chiang Mai', 'London', 'Singapore');
