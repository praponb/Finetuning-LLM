select id, product_id, quantity
from (
    select
        id,
        product_id,
        quantity,
        row_number() over (partition by product_id order by quantity desc, id) as row_num
    from inventory
) as r
where row_num = 1
