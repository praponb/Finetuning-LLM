select shipping_fee as value
from orders
intersect
select amount as value
from payments
where order_id in (2, 7)
