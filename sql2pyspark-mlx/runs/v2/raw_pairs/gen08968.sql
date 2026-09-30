select
    products.brand,
    products.category,
    max(inventory.quantity) - min(inventory.quantity) as quantity_range,
    count(inventory.quantity) as quantity_count
from inventory
inner join products on inventory.product_id = products.id
right outer join suppliers on products.supplier_id = suppliers.id
where products.stock_qty <= 192
group by products.brand, products.category;
