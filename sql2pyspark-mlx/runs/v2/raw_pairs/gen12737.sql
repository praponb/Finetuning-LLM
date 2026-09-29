select store_sales.id, store_sales.store_id, store_sales.units
from store_sales
where store_sales.revenue < 2100 and store_sales.product_id not in (select id from products where cost < 700)
order by store_id desc;
