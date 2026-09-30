with product_totals as (
    select product_id, min(units) as min_units
    from store_sales
    where units <= 24
    group by product_id
)
select p.brand, p.category, pt.min_units
from products p
inner join product_totals pt on p.id = pt.product_id
