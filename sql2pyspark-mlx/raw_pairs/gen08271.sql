select
    s.id,
    s.opened_date,
    s.sqft,
    s.store_name
from stores s
where s.region <> 'East' and not exists (select 1 from store_sales ss where ss.store_id = s.id)
