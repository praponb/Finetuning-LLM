with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        tag
    from carts
    lateral view outer explode(coupon_codes) x as tag
),
enriched as (
    -- Attach customers attributes
    select
        src.*,
        c.city,
        c.country,
        c.segment
    from flattened src
    left join customers c
        on src.customer_id <=> c.id
),
with_dims as (
    -- Attach customers attributes
    select
        src.customer_id,
        src.tag,
        p.name,
        p.signup_date
    from enriched src
    join customers p
        on src.customer_id <=> p.id
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat_ws('-', name, tag) as name_tag_key,
        last_day(signup_date) as signup_date_month_end,
        coalesce(tag, 'UNKNOWN') as resolved_tag
    from with_dims
    where customer_id is not null
),
with_dims_carts as (
    -- Attach customers attributes
    select a.*, p.country
    from base a
    left join customers p
        on a.customer_id <=> p.id
),
joined as (
    -- Attach customers attributes
    select
        a.*,
        p.segment,
        p.country as customer_country,
        p.age
    from with_dims_carts a
    join customers p
        on a.customer_id <=> p.id
    where p.is_active
)
select
    tag,
    name,
    signup_date,
    name_tag_key,
    signup_date_month_end,
    resolved_tag,
    country,
    segment,
    customer_country,
    case when segment rlike '^(Corporate|SMB)$' then 'GROUP_A' else 'CORE' end as segment_zone
from joined;
