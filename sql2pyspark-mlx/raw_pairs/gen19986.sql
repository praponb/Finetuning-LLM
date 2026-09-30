with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        inline(items)
    from carts
),
with_dims as (
    -- Attach customers attributes
    select
        e.customer_id,
        e.created_at,
        e.price,
        d.email,
        d.age
    from flattened e
    left join customers d
        on e.customer_id = d.id
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        customer_id,
        created_at,
        price,
        email,
        age,
        length(email) email_length,
        datediff(current_date(), created_at) days_since_created_at
    from with_dims
    where email like '%@startup.io'
),
joined as (
    -- Attach customers attributes
    select
        e.*,
        b.city,
        b.signup_date
    from filtered e
    left join customers b
        on e.customer_id = b.id
),
joined_carts as (
    -- Attach customers attributes
    select
        s.customer_id,
        s.email,
        s.email_length,
        s.days_since_created_at,
        s.city,
        p.email as customer_email
    from joined s
    left join customers p
        on s.customer_id = p.id
)
-- Join a dimension and aggregate
select
    d.segment,
    s.city as cart_city,
    count(*) as cnt,
    sum(s.email_length) as total_email_length,
    sum(s.days_since_created_at) as total_days_since_created_at,
    grouping_id() as grouping_level
from joined_carts s
inner join customers d on s.customer_id = d.id
group by cube (d.segment, s.city)
