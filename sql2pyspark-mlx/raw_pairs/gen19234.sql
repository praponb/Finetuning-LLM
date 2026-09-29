with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        pos + 1 as position,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    from carts
    lateral view outer posexplode(items) t as pos, elem
),
exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        position,
        product_id,
        qty,
        price,
        promo,
        explode_outer(coupon_codes) as code
    from lines
),
joined as (
    -- Attach customers attributes
    select
        s.customer_id,
        s.created_at,
        s.position,
        s.qty,
        s.price,
        d.signup_date,
        d.name
    from exploded s
    left join customers d
        on s.customer_id = d.id
),
joined_carts as (
    -- Attach customers attributes
    select
        s.customer_id,
        s.created_at,
        s.position,
        s.price,
        s.name,
        c.signup_date as customer_signup_date
    from joined s
    left join customers c
        on s.customer_id = c.id
    where c.country = 'JP'
),
with_dims as (
    -- Attach customers attributes
    select
        a.customer_id,
        a.price,
        a.name,
        d.age,
        d.country,
        d.name as customer_name
    from joined_carts a
    left join customers d
        on a.customer_id <=> d.id
),
with_dims_carts as (
    -- Attach customers attributes
    select
        a.*,
        p.name as customer_name_2,
        p.age as customer_age,
        p.credit_limit
    from with_dims a
    inner join customers p
        on a.customer_id = p.id
),
enriched as (
    -- Attach customers attributes
    select
        src.*,
        d.city,
        d.age as customer_age_2
    from with_dims_carts src
    left join customers d
        on src.customer_id = d.id
)
select
    customer_id,
    price,
    name,
    age,
    country,
    customer_name,
    credit_limit,
    city,
    customer_age_2
from enriched
