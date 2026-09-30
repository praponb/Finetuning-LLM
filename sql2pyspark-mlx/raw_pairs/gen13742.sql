select supplier_name
from suppliers
where length(supplier_name) >= 14
intersect
select status
from subscriptions
order by supplier_name;
