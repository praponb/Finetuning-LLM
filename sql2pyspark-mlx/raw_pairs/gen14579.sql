select
    year(last_restock) as last_restock_year,
    store_id,
    max(quantity) - min(quantity) as quantity_range,
    max(quantity) as max_quantity
from inventory
group by last_restock_year, store_id;
