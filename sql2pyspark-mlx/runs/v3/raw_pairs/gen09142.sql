select quantity, last_restock, cast(quantity as string) as quantity_num
from inventory
where product_id in (1, 3, 4);
