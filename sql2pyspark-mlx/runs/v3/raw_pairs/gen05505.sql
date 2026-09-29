select *, add_months(last_restock, 1) as last_restock_plus_months
from inventory
where last_restock > '2025-06-03';
