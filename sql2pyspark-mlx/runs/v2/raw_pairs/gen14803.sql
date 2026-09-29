select oi.id, oi.unit_price
from order_items oi
where oi.unit_price < 290 and oi.product_id in (select id from products where brand in ('Acme', 'Umbrella', 'Stark'))
order by oi.id;
