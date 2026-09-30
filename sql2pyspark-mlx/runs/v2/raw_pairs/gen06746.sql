select
    ss.id,
    ss.sale_date,
    ss.store_id,
    p.product_name,
    s.country,
    s.supplier_name
from store_sales ss
inner join products p on p.id = ss.product_id
full outer join suppliers s on p.supplier_id = s.id;
