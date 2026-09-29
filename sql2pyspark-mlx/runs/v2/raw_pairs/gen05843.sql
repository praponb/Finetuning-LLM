select id, discount
from orders
where discount < (select min(discount) from orders)
order by discount, id desc
