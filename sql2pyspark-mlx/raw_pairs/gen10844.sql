with inventory_by_product as (
    select product_id, avg(quantity) as avg_quantity, min(last_restock) as min_last_restock
    from inventory
    group by product_id
)
select
    p.cost,
    p.category,
    ibp.avg_quantity,
    ibp.min_last_restock
from products p
left join inventory_by_product ibp on p.id = ibp.product_id
order by cost;
