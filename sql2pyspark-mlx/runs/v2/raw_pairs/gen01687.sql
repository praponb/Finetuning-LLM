select
    ss.units,
    s.opened_date,
    s.sqft,
    s.store_name
from store_sales ss
inner join stores s on ss.store_id = s.id
where s.opened_date <= '2025-05-11'
