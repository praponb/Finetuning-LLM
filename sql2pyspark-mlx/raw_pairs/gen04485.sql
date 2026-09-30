with flattened as (
    select
        id,
        customer_id,
        created_at,
        attr_key,
        attr_value
    from carts
    lateral view explode(attributes) x as attr_key, attr_value
)
select
    id,
    attr_key,
    attr_value,
    row_number() over w_ordered as row_seq,
    last_value(attr_key) ignore nulls over (partition by year(created_at), customer_id order by created_at asc, id asc, attr_key asc rows between unbounded preceding and current row) as last_known_attr_key,
    first_value(attr_key) ignore nulls over w_recent as first_known_attr_key
from flattened
where created_at >= '2025-09-01'
window w_ordered as (partition by date_trunc('WEEK', created_at) order by created_at desc, id asc, attr_key asc), w_recent as (partition by customer_id order by created_at asc, id asc, attr_key asc rows between unbounded preceding and current row)
