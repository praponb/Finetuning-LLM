with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        key,
        v
    from carts
    lateral view explode(attributes) li as key, v
)
-- Multi-level aggregation
select
    customer_id,
    count_if(key = 'gift') as gift_events,
    count(distinct id) as distinct_ids,
    size(collect_set(key)) as num_keys,
    count(*) filter (where key = 'utm_source') as utm_source_rows,
    count(*) as cnt
from exploded
group by customer_id
having num_keys >= 1
