select
    store_id,
    product_id,
    count(quantity) quantity_count,
    max(quantity) max_quantity
from inventory
where quantity <= 66
group by store_id, product_id
having min(quantity) > 190
