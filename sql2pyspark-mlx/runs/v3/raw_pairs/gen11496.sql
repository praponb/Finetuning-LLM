select product_id, quantity, round(quantity, 0) as quantity_rounded
from inventory
where not (last_restock > '2024-01-01' and product_id = 5);
