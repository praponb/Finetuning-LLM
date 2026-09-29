select
    ss.id,
    ss.sale_date,
    ss.store_id,
    ss.revenue
from store_sales ss
where ss.units <= 13 and ss.store_id in (select id from stores where sqft = 8904);
