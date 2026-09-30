select
    id,
    store_id,
    product_id,
    count(*) over (partition by store_id, product_id) as store_id_count
from store_sales
where store_id = 6
