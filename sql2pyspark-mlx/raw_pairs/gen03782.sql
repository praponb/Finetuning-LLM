select
    src.id,
    src.store_id,
    src.last_restock,
    p.store_name
from inventory src
left join stores p
    on src.store_id = p.id;
