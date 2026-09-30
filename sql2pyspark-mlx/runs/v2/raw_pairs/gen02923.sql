with ranked as (
    select
        id,
        supplier_id,
        cost,
        category,
        dense_rank() over (partition by supplier_id order by cost desc) as rn
    from products
)
select *
from ranked
where rn <= 3;
