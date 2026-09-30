select
    id,
    quantity,
    unit_price,
    round(unit_price * 0.9, 2) as adjusted_unit_price
from order_items
where unit_price between 150 and 380 or product_id in (16, 29)
order by unit_price desc;
