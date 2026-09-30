select
    ss.store_id,
    ss.units,
    ss.revenue,
    s.city
from store_sales ss
inner join stores s on s.id = ss.store_id
