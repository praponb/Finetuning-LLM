select id, store_id
from store_sales
where product_id in (2, 9)
union all
select id, store_id
from store_sales
where store_id = 6
