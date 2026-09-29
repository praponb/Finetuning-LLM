select
    ss.id,
    ss.sale_date,
    ss.units,
    s.store_name
from store_sales ss
full outer join stores s on ss.store_id = s.id
where ss.sale_date < '2023-08-18' and s.city != 'Berlin'
