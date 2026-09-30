select store_id
from orders
where shipping_fee <= 16
union all
select store_id
from orders
where customer_id in (1, 19, 27);
