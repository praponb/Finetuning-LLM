select i.id, i.last_restock
from inventory i
where i.product_id not in (select id from products where array_contains(tags, 'eco'));
