select
    inventory.id,
    inventory.product_id,
    products.supplier_id,
    suppliers.rating
from inventory
left join products on products.id = inventory.product_id
join suppliers on products.supplier_id = suppliers.id
