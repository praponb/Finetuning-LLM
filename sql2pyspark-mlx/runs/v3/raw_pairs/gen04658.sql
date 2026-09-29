with ranked as (
    select
        id,
        store_id,
        quantity,
        product_id,
        row_number() over (partition by store_id order by quantity, id) as rnk
    from inventory
)
select *
from ranked
where rnk <= 1
