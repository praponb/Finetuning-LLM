select c.city, c.country, count(p.amount) as amount_count
from payments p
join orders o on p.order_id = o.id
left outer join customers c on o.customer_id = c.id
where c.name is not null
group by c.city, c.country;
