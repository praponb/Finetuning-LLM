select
    id,
    order_id,
    product_id,
    unit_price,
    round(unit_price / sum(unit_price) over (partition by order_id, product_id), 3) as unit_price_share
from order_items;
