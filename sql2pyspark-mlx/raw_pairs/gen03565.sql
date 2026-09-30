select
    ss.id,
    ss.units,
    ss.store_id,
    p.supplier_id,
    p.launch_date
from store_sales ss
full outer join products p on ss.product_id = p.id
where ss.product_id in (1, 2, 7, 10) and p.supplier_id is not null;
