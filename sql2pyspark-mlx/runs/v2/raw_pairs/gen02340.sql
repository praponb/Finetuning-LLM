with OrderTotals as (
    select order_id, round(avg(unit_price), 2) avg_unit_price
    from order_items
    group by order_id
)
select
    o.id,
    o.order_date,
    o.status,
    O.avg_unit_price
from orders o
inner join OrderTotals O on o.id = O.order_id
where O.avg_unit_price > 63
order by avg_unit_price
