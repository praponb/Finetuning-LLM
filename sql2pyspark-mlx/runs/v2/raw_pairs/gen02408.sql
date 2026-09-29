select i.id, i.quantity, i.last_restock
from inventory i
where i.product_id in (select id from products where stock_qty < 222)
order by quantity desc;
