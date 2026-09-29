select
    inventory.id,
    inventory.last_restock,
    products.brand,
    products.cost,
    stores.region,
    stores.store_name
from inventory
full outer join products on inventory.product_id = products.id
left join stores on inventory.store_id = stores.id;
