select stores.opened_date, inventory.product_id
from stores
left outer join inventory on inventory.store_id = stores.id
