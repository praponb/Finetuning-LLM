select last_restock
from inventory
where quantity <> 216
except
select last_restock
from inventory
where product_id in (1, 3, 5, 6);
