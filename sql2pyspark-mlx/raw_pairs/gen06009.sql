select store_id, datediff(current_date(), last_restock) / 365 as years_since_last_restock
from inventory
where store_id in (2, 3, 5) and product_id in (1, 3, 4, 6);
