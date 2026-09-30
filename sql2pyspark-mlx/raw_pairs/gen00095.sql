select last_restock, quantity
from inventory
where quantity = 122
union
select last_restock, quantity
from inventory
where product_id = 2
