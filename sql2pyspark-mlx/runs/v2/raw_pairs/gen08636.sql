select id, last_restock
from inventory
where last_restock >= '2023-10-01' and quantity = 135
order by 2 desc, id
limit 3;
