select
    ss.id,
    ss.product_id,
    ss.units,
    s.region,
    s.opened_date,
    substring(s.region, 1, 4) region_prefix
from store_sales ss
left outer join stores s on ss.store_id = s.id;
