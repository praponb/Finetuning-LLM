select id, quantity, product_id
from order_items
where product_id is null
union all
select id, quantity, product_id
from order_items
where unit_price >= 140
