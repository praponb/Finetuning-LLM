select id, store_id
from orders
where discount is not null
union
select id, store_id
from orders
where shipping_fee < 18
