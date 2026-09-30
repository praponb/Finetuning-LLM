with product_totals as (
    select product_id, avg(quantity) as avg_quantity
    from inventory
    group by product_id
)
select
    p.id,
    p.brand,
    p.supplier_id,
    pt.avg_quantity
from products p
left join product_totals pt on p.id = pt.product_id
where pt.avg_quantity > 200
order by p.id desc, supplier_id;
