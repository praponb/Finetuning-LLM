with inventory_by_product as (
    select product_id, sum(quantity) as total_quantity, min(quantity) as min_quantity
    from inventory
    where quantity >= 124
    group by product_id
)
select
    p.launch_date,
    p.supplier_id,
    ibp.total_quantity,
    ibp.min_quantity
from products p
join inventory_by_product ibp on p.id = ibp.product_id
order by total_quantity
