with product_totals as (
    select product_id, count(distinct unit_price) as unique_orders, round(avg(unit_price), 2) as avg_unit_price
    from order_items
    group by product_id
)
select p.cost, pt.unique_orders, pt.avg_unit_price
from products p
join product_totals pt on p.id = pt.product_id
order by pt.avg_unit_price;
