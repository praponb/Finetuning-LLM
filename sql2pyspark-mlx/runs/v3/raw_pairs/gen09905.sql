select last_restock, 'inventory' as source
from inventory
where quantity >= 82
union
select launch_date as last_restock, 'product' as source
from products
order by source;
