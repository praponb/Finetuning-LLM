select
    last_restock,
    quantity,
    product_id,
    datediff(current_date(), last_restock) / 7 as weeks_since_last_restock,
    round(quantity * 0.9, 2) as quantity_adjusted
from inventory
where quantity <= 82 and store_id in (3, 4, 5)
