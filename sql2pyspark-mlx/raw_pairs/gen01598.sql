select s.id, s.store_name
from stores s
where exists (select 1 from store_sales ss where ss.store_id = s.id and ss.product_id in (1, 6))
