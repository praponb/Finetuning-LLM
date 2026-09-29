select
    id,
    store_id,
    quantity,
    product_id
from (
    select *, row_number() over (partition by store_id order by quantity desc, id) as pos
    from inventory
    where quantity != 136
) as r
where pos <= 3;
