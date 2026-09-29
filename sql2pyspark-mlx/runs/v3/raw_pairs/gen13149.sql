select id, shipping_fee, status
from orders
where shipping_fee >= (select avg(shipping_fee) from orders)
