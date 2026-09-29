with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        line.product_id,
        line.price,
        line.promo
    from (
        select
            id,
            customer_id,
            created_at,
            coupon_codes,
            explode_outer(items) as line
        from carts
    ) x
),
exploded_carts as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        value
    from exploded
    lateral view outer explode(coupon_codes) x as value
),
with_dims as (
    -- Attach customers attributes
    select s.*, p.segment
    from exploded_carts s
    left join customers p
        on s.customer_id = p.id
    where upper(p.name) like '%ACK%'
),
remaining_keys as (
    -- Set difference on keys
    select id, customer_id
    from with_dims
    except distinct
    select id, customer_id
    from carts
    where created_at < '2025-01-30'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_dims s
    inner join remaining_keys k on s.id = k.id and s.customer_id = k.customer_id
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, lower(value) as value_lower
    from kept_rows
    where created_at >= timestamp '2025-03-01 12:00:00'
      and segment is not null
)
select
    id,
    created_at,
    value,
    segment
from base
order by created_at desc nulls first;
