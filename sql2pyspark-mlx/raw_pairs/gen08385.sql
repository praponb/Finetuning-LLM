select inventory.id, inventory.last_restock
from inventory
where inventory.store_id in (select id from stores where region = 'South')
order by inventory.last_restock desc, id asc
limit 10
