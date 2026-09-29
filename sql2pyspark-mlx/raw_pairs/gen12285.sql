select id, store_id
from inventory
qualify rank() over (partition by store_id order by last_restock desc) = 1;
