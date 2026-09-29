select id, quantity
from inventory
where quantity < (select min(quantity) from inventory where last_restock < '2024-02-01')
