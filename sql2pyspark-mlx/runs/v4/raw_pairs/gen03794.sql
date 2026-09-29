select
    ss.id,
    ss.store_id,
    s.region,
    p.supplier_id
from store_sales ss
left join stores s on ss.store_id = s.id
left join products p on ss.product_id = p.id
order by supplier_id, id
limit 20
