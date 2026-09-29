select
    inventory.id,
    inventory.store_id,
    products.supplier_id,
    products.brand,
    stores.opened_date
from inventory
inner join products on inventory.product_id = products.id
left outer join stores on inventory.store_id = stores.id
where products.stock_qty between 309 and 342;
