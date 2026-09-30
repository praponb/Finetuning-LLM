select
    id,
    store_id,
    quantity,
    last_restock
from (
    select *, row_number() over (partition by store_id order by quantity, id) as row_num
    from inventory
    where last_restock >= '2024-12-01'
) as r
where row_num <= 2
