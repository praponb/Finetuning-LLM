select
    id,
    order_id,
    quantity,
    product_id,
    greatest(quantity, unit_price) as greatest_quantity_unit_price,
    case when unit_price >= 400 then 'High' when unit_price >= 150 then 'Medium' else 'Low' end as unit_price_tier
from order_items
where (order_id = 12 and order_id in (22, 40)) or quantity > 7
