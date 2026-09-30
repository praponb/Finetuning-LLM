select o.id, o.store_id, o.shipping_fee
from orders o
where o.customer_id in (select id from customers where upper(trim(name)) = 'ALICE WONG');
