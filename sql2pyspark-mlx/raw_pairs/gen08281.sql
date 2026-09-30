select last_restock
from inventory
where quantity between 195 and 208 and (quantity is null or store_id in (3, 4, 6));
