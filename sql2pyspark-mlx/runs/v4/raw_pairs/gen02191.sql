with product_stats as (
    select product_id, count(*) as num_inventory
    from inventory
    where store_id in (1, 2, 4, 5)
    group by product_id
)
select
    p.id,
    p.launch_date,
    p.product_name,
    ps.num_inventory
from products p
join product_stats ps on p.id = ps.product_id
order by p.launch_date desc;
