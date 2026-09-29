select product_id, order_id
from order_items
where order_id in (7, 16, 27, 34)
union all
select product_id, order_id
from order_items
where quantity != 6
order by product_id desc
