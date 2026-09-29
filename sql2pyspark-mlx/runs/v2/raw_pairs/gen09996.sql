with inventory_by_product as (
    select product_id, avg(quantity) as avg_quantity, count(*) as total_count
    from inventory
    where store_id in (1, 3, 5, 6)
    group by product_id
)
select
    p.launch_date,
    p.product_name,
    ibp.avg_quantity,
    ibp.total_count
from products p
join inventory_by_product ibp on p.id = ibp.product_id
where ibp.avg_quantity >= 110
order by total_count;
