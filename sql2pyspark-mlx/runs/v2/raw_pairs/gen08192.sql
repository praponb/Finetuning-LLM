with StoreTotals as (
    select store_id, max(last_restock) as latest_last_restock, avg(quantity) as avg_quantity
    from inventory
    group by store_id
)
select
    s.id,
    s.opened_date,
    s.store_name,
    S.latest_last_restock,
    S.avg_quantity
from stores s
join StoreTotals S on s.id = S.store_id
where S.avg_quantity > 190
