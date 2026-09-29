with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        value
    from carts
    lateral view outer explode(attributes) t as key, value
),
enriched as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.value,
        d.age
    from exploded a
    inner join customers d
        on a.customer_id <=> d.id
    where d.segment not in ('Consumer', 'Corporate')
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        round(age, 1) as age_rounded,
        round(age * 1.15, 2) as age_scaled
    from enriched
    where age <= 39
),
metrics as (
    -- Window metrics over several different partitions
    select *, round(age / sum(age) over (partition by customer_id), 4) as age_share
    from base
),
joined as (
    -- Attach customers attributes
    select
        e.customer_id,
        e.age_rounded,
        b.country,
        b.signup_date,
        b.name
    from metrics e
    left join customers b
        on e.customer_id = b.id
),
remaining_keys as (
    -- Set difference on keys
    select customer_id
    from joined
    except all
    select customer_id
    from carts
    where customer_id is not null
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from joined s
    inner join remaining_keys k on s.customer_id = k.customer_id
),
enriched_carts as (
    -- Attach customers attributes
    select
        src.*,
        c.segment,
        c.email
    from kept_rows src
    left join customers c
        on src.customer_id = c.id
)
-- Join a dimension and aggregate
select
    d.city,
    s.segment as cart_segment,
    count(*) as cnt,
    max(s.age_rounded) as max_age_rounded,
    grouping_id() as grouping_level
from enriched_carts s
left join customers d on s.customer_id = d.id
group by cube (d.city, s.segment);
