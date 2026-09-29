select products.id, products.stock_qty, products.cost
from products
where not exists (select 1 from inventory where inventory.product_id = products.id)
