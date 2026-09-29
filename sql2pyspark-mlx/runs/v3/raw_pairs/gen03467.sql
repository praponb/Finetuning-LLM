select
    i.id,
    i.product_id,
    s.store_name,
    s.region,
    p.supplier_id,
    p.product_name
from inventory i
left join stores s on i.store_id = s.id
left join products p on i.product_id = p.id;
