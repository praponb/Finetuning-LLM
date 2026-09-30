select
    product_id,
    quantity,
    last_restock,
    datediff(current_date(), last_restock) as days_since_last_restock
from inventory
where store_id = 6 and product_id in (8, 10) and product_id in (5, 10);
