select i.id, i.last_restock, i.product_id
from inventory i
where i.product_id in (8, 10) and i.product_id in (select id from products where cost < 700)
order by id desc
