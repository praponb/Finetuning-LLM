select inventory.id, inventory.store_id
from inventory
where inventory.store_id = 5 and inventory.store_id in (select id from stores where region = 'West');
