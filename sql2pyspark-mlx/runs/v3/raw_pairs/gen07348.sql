select
    ss.id,
    ss.product_id,
    s.city,
    p.product_name
from store_sales ss
left join stores s on ss.store_id = s.id
left join products p on ss.product_id = p.id
where s.sqft > 8315
