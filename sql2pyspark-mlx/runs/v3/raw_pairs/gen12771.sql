select
    ss.id,
    ss.revenue,
    ss.sale_date,
    p.supplier_id,
    s.store_name,
    floor(ss.units) as units_whole
from store_sales ss
inner join products p on ss.product_id = p.id
inner join stores s on ss.store_id = s.id
