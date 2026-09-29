select id, last_restock
from inventory
order by last_restock, id
limit 3;
