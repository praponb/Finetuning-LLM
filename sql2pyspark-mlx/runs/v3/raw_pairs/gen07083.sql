select count(*) as total_count, sum(cost) as total_cost
from products
where cost between 560 and 650 and supplier_id = 19 and supplier_id in (7, 14)
order by total_cost
