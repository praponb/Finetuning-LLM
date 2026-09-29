select products.category, max(inventory.quantity) as max_quantity
from inventory
inner join products on products.id = inventory.product_id
full outer join stores on inventory.store_id = stores.id
group by products.category
