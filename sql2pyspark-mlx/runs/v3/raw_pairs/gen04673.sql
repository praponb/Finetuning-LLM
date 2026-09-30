select
    ss.id,
    ss.units,
    s.city,
    s.store_name,
    s.sqft
from store_sales ss
inner join stores s on ss.store_id = s.id;
