select date_format(last_restock, 'yyyy-MM') as last_restock_month, max(quantity) - min(quantity) as quantity_range
from inventory
group by last_restock_month;
