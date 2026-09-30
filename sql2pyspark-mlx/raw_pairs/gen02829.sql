with ranked as (
    select
        id,
        store_id,
        quantity,
        product_id,
        rank() over (partition by store_id order by quantity desc) as rn
    from inventory
)
select *
from ranked
where rn <= 1
order by rn desc;
