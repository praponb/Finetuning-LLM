select
    id,
    product_id,
    unit_price,
    round(unit_price / sum(unit_price) over (partition by product_id), 3) unit_price_share
from order_items
order by 3 desc;
