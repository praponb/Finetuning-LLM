select *, year(last_restock) as last_restock_year
from inventory
where quantity is not null or quantity > 189
