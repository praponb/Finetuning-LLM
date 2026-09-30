select
    store_id,
    product_id,
    count(distinct quantity) as unique_products,
    count(*) as total_count
from inventory
where quantity is not null
group by cube(store_id, product_id);
