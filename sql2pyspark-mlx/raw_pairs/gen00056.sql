with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        inline(items)
    from carts
)
-- Multi-level aggregation
select
    product_id,
    customer_id,
    (unix_timestamp(max(created_at)) - unix_timestamp(min(created_at))) / 60 as duration_min,
    size(collect_set(promo)) as num_promos,
    count_if(promo = 'WELCOME5') as welcome5_events
from lines
group by product_id, customer_id;
