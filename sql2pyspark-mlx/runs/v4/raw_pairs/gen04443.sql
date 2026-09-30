select price
from products
intersect
select shipping_fee
from orders
where shipping_fee < 34
order by price desc;
