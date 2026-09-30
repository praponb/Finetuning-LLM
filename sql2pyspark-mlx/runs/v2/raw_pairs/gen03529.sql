select
    ss.id,
    ss.sale_date,
    ss.product_id,
    s.sqft,
    s.store_name
from store_sales ss
inner join stores s on ss.store_id = s.id
where not (s.store_name like 'Dow%' or s.region <> 'South');
