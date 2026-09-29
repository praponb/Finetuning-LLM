select store_id, id
from inventory
intersect
select store_id, id
from inventory
where year(last_restock) = 2024;
