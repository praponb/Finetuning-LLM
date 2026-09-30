with Base as (
    select *, round(quantity * 0.85, 2) as quantity_scaled
    from inventory
    where quantity between 101 and 239
),
summary as (
    select store_id, max(quantity_scaled) - min(quantity_scaled) as quantity_scaled_range, min(quantity_scaled) as min_quantity_scaled
    from Base
    group by store_id
)
select *
from summary
order by min_quantity_scaled desc, quantity_scaled_range desc, store_id asc
limit 5
