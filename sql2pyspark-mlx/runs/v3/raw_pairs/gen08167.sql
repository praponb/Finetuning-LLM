select shipping_fee, 'order' source
from orders
union all
select cost shipping_fee, 'product' source
from products
where brand in ('Umbrella', 'Stark')
order by shipping_fee;
