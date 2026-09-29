select
    ss.sale_date,
    ss.store_id,
    p.category,
    p.launch_date,
    s.country
from store_sales ss
full outer join products p on ss.product_id = p.id
left join suppliers s on p.supplier_id = s.id;
