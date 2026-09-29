select
    id,
    quantity,
    store_id,
    round(quantity, 2) as quantity_rounded
from inventory
where product_id = 6
order by store_id desc;
