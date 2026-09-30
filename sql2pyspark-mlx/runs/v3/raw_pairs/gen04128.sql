select
    product_id,
    store_id,
    max(quantity) - min(quantity) as quantity_range,
    max(last_restock) as last_last_restock,
    count(distinct quantity) as unique_stores
from inventory
where product_id in (2, 3, 4, 7) and product_id in (5, 9, 10)
group by cube(product_id, store_id)
