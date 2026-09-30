select stores.id, stores.city
from stores
where not exists (select 1 from orders where orders.store_id = stores.id and orders.shipping_fee > 21)
order by 1;
