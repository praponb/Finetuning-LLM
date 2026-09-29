select products.category, count(*) as num_inventory
from inventory
inner join products on products.id = inventory.product_id
inner join suppliers on products.supplier_id = suppliers.id
group by products.category
