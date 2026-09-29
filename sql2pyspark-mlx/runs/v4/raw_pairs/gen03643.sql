with filtered as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        last_restock,
        day(last_restock) as last_restock_day
    from inventory
    where store_id in (2, 3, 5)
)
-- QUALIFY-style window filter
select *
from (
    select *, rank() over (partition by store_id order by last_restock desc) as rn
    from filtered
) x
where rn = 1;
