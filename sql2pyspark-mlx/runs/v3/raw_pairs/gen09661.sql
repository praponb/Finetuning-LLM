select
    products.stock_qty,
    inventory.store_id,
    inventory.product_id,
    inventory.quantity
from products
left outer join inventory on products.id = inventory.product_id;
