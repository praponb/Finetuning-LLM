select
    ss.id,
    ss.store_id,
    ss.product_id,
    s.store_name,
    s.city,
    s.sqft
from store_sales ss
inner join stores s on ss.store_id = s.id
order by s.city;
