with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        attr_value
    from carts
    lateral view explode(attributes) t as k, attr_value
),
exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        k,
        attr_value,
        element
    from flattened
    lateral view explode(coupon_codes) x as element
),
remaining_keys as (
    -- Set intersection on keys
    select customer_id
    from exploded
    intersect
    select customer_id
    from carts
    where created_at >= '2025-07-01'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from exploded s
    inner join remaining_keys k on s.customer_id = k.customer_id
),
enriched as (
    -- Attach customers attributes
    select
        e.customer_id,
        e.created_at,
        e.element,
        b.age
    from reconciled e
    inner join customers b
        on e.customer_id <=> b.id
    where b.is_active
)
-- Join a dimension and aggregate
select
    d.country,
    d.city,
    count(*) as rows_total,
    max(src.age) as max_age
from enriched src
inner join customers d on src.customer_id = d.id
group by rollup (d.country, d.city)
order by max_age desc
