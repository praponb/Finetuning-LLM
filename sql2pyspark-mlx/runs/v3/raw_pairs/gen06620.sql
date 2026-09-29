select
    id,
    product_id,
    quantity,
    last_restock
from (
    select
        id,
        product_id,
        quantity,
        last_restock,
        row_number() over (partition by product_id order by quantity, id) as row_num
    from inventory
) as r
where row_num <= 2
