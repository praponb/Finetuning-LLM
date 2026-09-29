select id, product_id
from store_sales
intersect all
select id, product_id
from store_sales
where product_id in (4, 5, 7, 9);
