with lines as (
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        v
    from carts
    lateral view outer explode(attributes) li as attr_key, v
),
flattened as (
    select
        id,
        attr_key,
        tag
    from lines
    lateral view outer explode(coupon_codes) e as tag
),
base as (
    select
        *,
        lower(attr_key) as attr_key_lower,
        coalesce(attr_key, 'UNKNOWN') as attr_key_clean
    from flattened
    where tag is not null
),
valid as (
    select
        attr_key,
        tag,
        attr_key_lower,
        attr_key_clean,
        upper(tag) as tag_upper,
        length(attr_key) as attr_key_length
    from base
    where tag is not null
),
with_windows as (
    select *, count(attr_key_length) over (partition by tag) as tag_count_attr_key_length
    from valid
)
select
    tag,
    count(*) as event_count,
    round(sum(attr_key_length) / nullif(count(case when tag = 'BUNDLE' then 1 end), 0), 2) as attr_key_length_per_bundle,
    count(case when attr_key = 'device' then 1 end) as device_count,
    max(attr_key_length) as max_attr_key_length,
    round(sum(tag_count_attr_key_length) / nullif(count(case when tag = 'SPRING10' then 1 end), 0), 2) as tag_count_attr_key_length_per_spring10
from with_windows
group by tag
having max(attr_key_length) > 0
order by max_attr_key_length desc;
