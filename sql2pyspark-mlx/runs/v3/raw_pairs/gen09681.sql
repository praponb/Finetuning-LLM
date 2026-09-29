with product_summary as (
    select product_id, max(revenue) as max_revenue, max(units) - min(units) as units_range
    from store_sales
    group by product_id
)
select
    p.product_name,
    p.brand,
    ps.max_revenue,
    ps.units_range
from products p
left join product_summary ps on p.id = ps.product_id
