select
    id,
    last_restock,
    quantity,
    product_id
from inventory
where quantity >= 119
order by quantity desc;
