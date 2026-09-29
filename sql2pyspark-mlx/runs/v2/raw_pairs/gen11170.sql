select
    id,
    product_id,
    order_id,
    count(*) over (partition by product_id, order_id) as product_id_count
from order_items
