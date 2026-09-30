select
    inventory.id,
    inventory.last_restock,
    products.cost,
    products.price,
    suppliers.country,
    suppliers.rating
from inventory
right join products on inventory.product_id = products.id
full outer join suppliers on products.supplier_id = suppliers.id
where inventory.store_id in (3, 6) and products.stock_qty is not null;
