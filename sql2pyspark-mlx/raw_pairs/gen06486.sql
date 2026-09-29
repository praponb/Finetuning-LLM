select
    id,
    order_id,
    product_id,
    unit_price,
    rank() over (partition by order_id, product_id order by unit_price) as unit_price_rank
from order_items
where product_id = 5;
