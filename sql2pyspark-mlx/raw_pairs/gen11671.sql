select id
from order_items
except distinct
select id
from order_items
where order_id = 12
