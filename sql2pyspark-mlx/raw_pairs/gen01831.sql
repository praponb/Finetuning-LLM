select id, cost, price
from products
where product_name like '%Smart%' and launch_date < '2025-02-05';
