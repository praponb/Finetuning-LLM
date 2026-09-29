select order_date, shipping_fee
from orders
where status not in ('SHIPPED', 'CANCELLED') and store_id is not null;
