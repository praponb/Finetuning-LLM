select id, store_id, shipping_fee
from orders
where status in ('COMPLETED', 'SHIPPED', 'CANCELLED') and amount >= 1010
