select order_id, product_id
from order_items
where quantity between 6 and 7
union all
select order_id, product_id
from order_items
where quantity between 4 and 6
