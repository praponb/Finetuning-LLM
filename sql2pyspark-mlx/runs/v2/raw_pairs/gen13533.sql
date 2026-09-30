select month(last_restock) as last_restock_month, count(quantity) as quantity_count
from inventory
group by 1
