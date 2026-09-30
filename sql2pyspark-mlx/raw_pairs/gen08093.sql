select shipping_fee, 'order' kind
from orders
where shipping_fee is not null
union all
select unit_price, 'order_item' kind
from order_items
where unit_price is not null;
