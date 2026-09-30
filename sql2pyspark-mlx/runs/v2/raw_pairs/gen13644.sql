select p.id, p.price, p.product_name
from products p
where p.launch_date < '2025-10-01' and p.id in (select product_id from store_sales where product_id in (2, 5, 7, 9));
