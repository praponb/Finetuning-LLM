select *, date_trunc('week', last_restock) as last_restock_period
from inventory
where quantity = 209 or quantity <> 220;
