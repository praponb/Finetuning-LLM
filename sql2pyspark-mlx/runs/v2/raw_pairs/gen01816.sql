select ss.sale_date, s.region, s.sqft
from store_sales ss
inner join stores s on s.id = ss.store_id
where s.region in ('South', 'East');
