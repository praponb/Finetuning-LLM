select launch_date
from products
union all
select order_date
from orders
where customer_id in (7, 29, 38)
order by launch_date desc;
