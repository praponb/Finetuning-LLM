select s.id, s.city, s.opened_date
from stores s
where s.store_name is not null and exists (select 1 from store_sales ss where ss.store_id = s.id)
