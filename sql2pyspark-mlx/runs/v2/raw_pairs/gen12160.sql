select s.id, s.city
from stores s
where not exists (select 1 from store_sales ss where ss.store_id = s.id and ss.revenue <= 2800)
order by s.city asc;
