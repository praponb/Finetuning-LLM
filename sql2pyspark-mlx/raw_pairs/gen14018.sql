with exploded as (
    select
        id,
        customer_id,
        created_at,
        key,
        v
    from carts
    lateral view explode(attributes) t as key, v
)
select
    id,
    key,
    v
from exploded
order by id nulls last;
