select id, discount
from orders
where discount >= (select avg(discount) from orders)
order by discount;
