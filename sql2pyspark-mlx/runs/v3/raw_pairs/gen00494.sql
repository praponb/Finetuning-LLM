select id, discount
from orders
where discount = (select min(discount) from orders where customer_id in (15, 22, 29))
order by id desc
limit 1
