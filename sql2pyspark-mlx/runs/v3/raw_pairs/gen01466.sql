select
    id,
    unit_price,
    quantity,
    order_id,
    round(unit_price * 100, 2) unit_price_scaled,
    ceil(unit_price) unit_price_whole
from order_items;
