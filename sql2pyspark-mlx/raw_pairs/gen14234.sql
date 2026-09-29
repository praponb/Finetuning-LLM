with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        v
    from carts
    lateral view explode(attributes) li as attr_key, v
),
exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        attr_key,
        v,
        element
    from lines
    lateral view outer explode(coupon_codes) e as element
),
matched_keys as (
    -- Set intersection on keys
    select id, customer_id
    from exploded
    intersect
    select id, customer_id
    from carts
    where to_date(created_at) < '2025-02-26'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from exploded s
    inner join matched_keys k on s.id = k.id and s.customer_id = k.customer_id
),
with_dims as (
    -- Attach customers attributes
    select
        src.id,
        src.customer_id,
        src.element,
        d.country,
        d.email
    from reconciled src
    inner join customers d
        on src.customer_id = d.id
    where d.credit_limit <= 15700
),
kept_keys as (
    -- Set difference on keys
    select customer_id, id
    from with_dims
    except
    select customer_id, id
    from carts
    where to_date(created_at) >= '2024-10-19'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_dims s
    inner join kept_keys k on s.customer_id = k.customer_id and s.id = k.id
)
-- Multi-level aggregation
select
    element,
    count(*) as num_rows,
    sort_array(collect_set(country)) as country_list,
    count(*) as row_count,
    count(*) filter (where element = 'SPRING10') as spring10_rows
from kept_rows
group by element
having spring10_rows >= 1
