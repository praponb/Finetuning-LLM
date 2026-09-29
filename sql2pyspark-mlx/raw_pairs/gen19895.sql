with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        explode_outer(coupon_codes) as tag
    from carts
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, substring(tag, 1, 2) as tag_prefix
    from flattened
    where customer_id in (8, 11, 30, 36)
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        upper(tag) as tag_upper,
        upper(tag) as tag_upper_2,
        coalesce(tag, 'NONE') as resolved_tag
    from filtered
    where customer_id is not null
),
with_dims as (
    -- Attach customers attributes
    select
        s.*,
        c.signup_date,
        c.segment
    from cleaned s
    join customers c
        on s.customer_id = c.id
    where c.segment is not null
),
joined as (
    -- Attach customers attributes
    select
        s.*,
        b.name,
        b.email
    from with_dims s
    left join customers b
        on s.customer_id = b.id
)
select
    id,
    customer_id,
    tag,
    tag_prefix,
    tag_upper,
    tag_upper_2,
    signup_date,
    segment,
    email
from joined;
