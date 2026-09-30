select id, store_id
from store_sales
where store_id in (3, 4)
union
select id, store_id
from store_sales
where units <= 27;
