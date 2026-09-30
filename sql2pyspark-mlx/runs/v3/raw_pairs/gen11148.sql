select id, revenue
from store_sales
where product_id in (2, 7)
order by 1 desc;
