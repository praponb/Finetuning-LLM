select id, last_restock, product_id
from inventory
where last_restock = (select max(last_restock) from inventory where quantity > 224);
