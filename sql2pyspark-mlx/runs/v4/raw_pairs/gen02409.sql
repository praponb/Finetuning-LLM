select customer_id, store_id, amount + shipping_fee as amount_with_shipping
from orders
where customer_id is null and status <> 'RETURNED';
