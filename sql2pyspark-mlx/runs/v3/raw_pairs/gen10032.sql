select
    inventory.last_restock,
    inventory.store_id,
    stores.sqft,
    stores.region,
    stores.store_name
from inventory
full outer join stores on inventory.store_id = stores.id
where inventory.store_id = 1 and year(inventory.last_restock) = 2024
order by stores.sqft desc;
