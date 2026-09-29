select shipping_fee
from orders
union
select cost
from products
order by shipping_fee;
