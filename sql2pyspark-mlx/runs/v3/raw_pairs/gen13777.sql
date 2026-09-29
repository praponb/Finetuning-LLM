select
    inventory.id,
    inventory.quantity,
    inventory.last_restock,
    stores.city,
    stores.store_name,
    products.stock_qty
from inventory
join stores on inventory.store_id = stores.id
join products on inventory.product_id = products.id
where stores.store_name is not null
order by stores.city;
