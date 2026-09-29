select max(quantity) as max_quantity
from inventory
where quantity < 187 and month(last_restock) in (7, 9, 10) and last_restock > '2024-09-01'
order by max_quantity desc;
