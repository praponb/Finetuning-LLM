with product_totals as (
    select product_id, max(quantity) - min(quantity) as quantity_range
    from order_items
    where product_id is not null
    group by product_id
)
select p.launch_date, p.category, pt.quantity_range
from products p
left join product_totals pt on p.id = pt.product_id
where pt.quantity_range > 68
order by quantity_range asc
