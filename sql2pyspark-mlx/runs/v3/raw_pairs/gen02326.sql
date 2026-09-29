select date_format(last_restock, 'yyyy-MM') as last_restock_month, min(last_restock) as first_last_restock
from inventory
group by last_restock_month;
