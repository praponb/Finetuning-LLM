with product_totals as (
    select product_id, round(avg(unit_price), 2) avg_unit_price
    from order_items
    where unit_price >= 200
    group by product_id
)
select p.id, p.price, pt.avg_unit_price
from products p
left join product_totals pt on p.id = pt.product_id
order by p.price
