select
    ss.id,
    ss.store_id,
    s.region,
    p.brand,
    p.category
from store_sales ss
left join stores s on s.id = ss.store_id
left join products p on ss.product_id = p.id;
