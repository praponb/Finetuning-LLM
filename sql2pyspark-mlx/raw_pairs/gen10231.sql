select i.id, i.quantity, i.product_id
from inventory i
where i.last_restock between '2024-01-01' and '2024-04-01' and i.product_id in (select id from products where cost is not null);
