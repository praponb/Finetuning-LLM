select
    s.id,
    s.sqft,
    s.opened_date,
    s.city
from stores s
where s.opened_date between '2023-05-01' and '2025-10-26' and exists (select 1 from store_sales ss where ss.store_id = s.id and ss.store_id = 2);
