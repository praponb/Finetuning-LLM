with store_stats as (
    select store_id, avg(quantity) as avg_quantity
    from inventory
    where product_id in (2, 4, 7, 9)
    group by store_id
)
select stores.id, stores.region, store_stats.avg_quantity
from stores
join store_stats on stores.id = store_stats.store_id
where store_stats.avg_quantity > 170;
