select id, store_id, status
from orders
where shipping_fee >= 14
intersect
select id, store_id, status
from orders
where channel = 'partner'
order by status desc;
