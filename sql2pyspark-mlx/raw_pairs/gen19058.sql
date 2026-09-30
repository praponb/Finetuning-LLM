select id
from order_items
except
select id
from order_items
where order_id in (17, 21, 30, 37)
