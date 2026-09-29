with product_summary as (
    select product_id, max(revenue) as max_revenue
    from store_sales
    group by product_id
)
select
    p.id,
    p.supplier_id,
    p.cost,
    ps.max_revenue
from products p
inner join product_summary ps on p.id = ps.product_id
order by supplier_id;
