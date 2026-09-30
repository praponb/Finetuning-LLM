select
    inventory.id,
    inventory.last_restock,
    inventory.store_id,
    stores.opened_date,
    products.stock_qty
from inventory
left join stores on inventory.store_id = stores.id
right join products on products.id = inventory.product_id
where products.stock_qty >= 274;
