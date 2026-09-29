select
    id,
    quantity,
    last_restock,
    product_id
from inventory
where last_restock between '2025-02-09' and '2025-08-01' and year(last_restock) = 2023
order by quantity desc, id
limit 5;
