select inventory.id, inventory.store_id
from inventory
where inventory.quantity between 66 and 151 and inventory.store_id in (select id from stores where sqft <= 15688)
order by store_id desc, id
limit 10
