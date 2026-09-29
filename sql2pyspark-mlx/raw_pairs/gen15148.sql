select last_restock, store_id, year(last_restock) as last_restock_year
from inventory
where not (last_restock between '2024-07-01' and '2025-01-01' and quantity >= 233)
order by store_id desc, last_restock_year desc
