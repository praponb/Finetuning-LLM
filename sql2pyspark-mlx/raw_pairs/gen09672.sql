select
    id,
    product_id,
    quantity,
    lead(quantity, 2, 0) over (partition by product_id order by quantity, id) as next_quantity
from order_items
where product_id = 26
order by next_quantity, id
