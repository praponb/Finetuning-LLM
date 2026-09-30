with src as (
    select *, round(unit_price, 0) as unit_price_rounded
    from order_items
    where product_id is null and quantity > 7
),
summary as (
    select product_id, sum(unit_price_rounded) as total_unit_price_rounded, count(quantity) as quantity_count
    from src
    group by product_id
)
select product_id, total_unit_price_rounded
from summary;
