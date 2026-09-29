select order_id, avg_unit_price
from (
    select order_id, round(avg(unit_price), 2) as avg_unit_price
    from order_items
    group by order_id
) as t
where avg_unit_price < 42
order by avg_unit_price desc
