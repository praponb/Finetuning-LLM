select product_name
from products
where price < 350
union all
select product_name
from products
where supplier_id in (21, 31, 34, 35);
