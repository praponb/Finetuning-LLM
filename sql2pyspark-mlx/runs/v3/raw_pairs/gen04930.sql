select revenue
from store_sales
intersect
select price
from products
where supplier_id is not null
order by revenue;
