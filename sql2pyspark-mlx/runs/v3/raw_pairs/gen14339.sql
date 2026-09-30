select ss.id, ss.units
from store_sales ss
where ss.revenue < 3700 and ss.product_id in (select id from products where supplier_id in (3, 9, 38))
order by id desc;
