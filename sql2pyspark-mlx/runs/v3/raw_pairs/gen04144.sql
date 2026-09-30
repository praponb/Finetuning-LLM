select
    product_id,
    last_restock,
    store_id,
    ceil(quantity) as quantity_whole,
    floor(quantity) as quantity_whole_2
from inventory
order by quantity_whole_2 desc, 4 desc;
