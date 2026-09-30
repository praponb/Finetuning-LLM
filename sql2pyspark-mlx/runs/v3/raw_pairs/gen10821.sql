select id, discount, status
from orders
where discount > (select max(discount) from orders where shipping_fee <= 19)
