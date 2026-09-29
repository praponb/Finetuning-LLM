with store_stats as (
    select store_id, min(last_restock) as first_last_restock
    from inventory
    where quantity < 169
    group by store_id
)
select
    s.id,
    s.opened_date,
    s.sqft,
    ss.first_last_restock
from stores s
inner join store_stats ss on s.id = ss.store_id;
