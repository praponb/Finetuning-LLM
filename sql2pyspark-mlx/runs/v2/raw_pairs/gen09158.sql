select inventory.id, inventory.store_id, products.launch_date
from inventory
left join products on inventory.product_id = products.id
order by inventory.store_id desc, id desc;
