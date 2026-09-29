with product_stats as (
    select product_id, max(quantity) - min(quantity) as quantity_range
    from inventory
    where last_restock between '2023-03-06' and '2025-03-01'
    group by product_id
)
select p.id, p.launch_date, ps.quantity_range
from products p
inner join product_stats ps on p.id = ps.product_id
where ps.quantity_range >= 72;
