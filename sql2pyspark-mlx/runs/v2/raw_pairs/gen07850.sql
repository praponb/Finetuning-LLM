select
    inventory.id,
    inventory.store_id,
    inventory.quantity,
    stores.opened_date
from inventory
left join stores on inventory.store_id = stores.id
where inventory.product_id in (3, 10) and inventory.product_id in (3, 5, 9, 10)
order by inventory.store_id, id
limit 50
