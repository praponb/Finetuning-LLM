select s.id, s.opened_date
from stores s
where s.sqft >= 6148 and exists (select 1 from store_sales ss where ss.store_id = s.id);
