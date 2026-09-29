select
    id,
    last_restock,
    store_id,
    product_id,
    date_format(last_restock, 'yyyy-MM') last_restock_label,
    round(quantity * 0.85, 2) quantity_scaled
from inventory
order by 5 asc
