select
    id,
    product_id,
    last_restock,
    quantity
from (
    select
        id,
        product_id,
        last_restock,
        quantity,
        row_number() over (partition by product_id order by last_restock, id) as row_num
    from inventory
    where product_id in (4, 5, 6, 9)
) as r
where row_num = 1;
