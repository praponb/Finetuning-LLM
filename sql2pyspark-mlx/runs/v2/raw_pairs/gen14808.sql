select *, datediff(current_date(), last_restock) as days_since_last_restock
from inventory
where product_id in (3, 4, 8)
order by days_since_last_restock desc, id desc;
