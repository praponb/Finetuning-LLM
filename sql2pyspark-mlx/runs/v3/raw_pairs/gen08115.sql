select
    s.id,
    s.opened_date,
    s.sqft,
    s.store_name
from stores s
where exists (select 1 from inventory i where i.store_id = s.id);
