with product_totals as (
    select product_id, round(avg(units), 2) as avg_units
    from store_sales
    group by product_id
)
select
    p.id,
    p.product_name,
    p.launch_date,
    pt.avg_units
from products p
join product_totals pt on p.id = pt.product_id
where pt.avg_units > 30;
