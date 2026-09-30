select
    last_restock,
    store_id,
    product_id,
    datediff(current_date(), last_restock) / 7 as weeks_since_last_restock
from inventory
order by weeks_since_last_restock;
