select
    id,
    product_id,
    order_id,
    floor(unit_price) as unit_price_whole
from order_items
where (order_id = 24 and product_id is not null) or quantity <= 4
order by id desc
limit 5
