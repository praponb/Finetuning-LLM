select
    ss.id,
    ss.units,
    ss.store_id,
    s.region
from store_sales ss
join stores s on s.id = ss.store_id
where ss.store_id in (3, 4, 5);
