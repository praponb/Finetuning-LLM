select id, product_name
from products
where launch_date >= '2025-08-13' and category <> 'Home'
order by id desc, product_name asc;
