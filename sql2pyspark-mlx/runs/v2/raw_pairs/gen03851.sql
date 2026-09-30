select status, customer_id
from orders
where shipping_fee between 13 and 21
union all
select status, customer_id
from orders
where order_date <= '2024-07-25';
