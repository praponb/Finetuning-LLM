with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        key,
        attr_value
    from carts
    lateral view outer explode(attributes) e as key, attr_value
),
exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        key,
        attr_value,
        value
    from flattened
    lateral view explode(coupon_codes) e as value
),
with_dims as (
    -- Attach customers attributes
    select
        e.*,
        c.country,
        c.email,
        c.signup_date
    from exploded e
    left join customers c
        on e.customer_id = c.id
),
enriched as (
    -- Attach customers attributes
    select
        a.customer_id,
        a.email,
        a.signup_date,
        c.country as customer_country
    from with_dims a
    join customers c
        on a.customer_id <=> c.id
    where length(c.name) < 10
),
joined as (
    -- Attach customers attributes
    select
        s.customer_id,
        s.email,
        s.signup_date,
        s.customer_country,
        c.signup_date as customer_signup_date,
        c.name,
        c.email as customer_email
    from enriched s
    left join customers c
        on s.customer_id = c.id
)
-- Multi-level aggregation
select
    date_trunc('year', customer_signup_date) as year_start,
    count(*) as num_rows,
    count(case when customer_country = 'TH' then 1 end) as th_count,
    count(*) filter (where customer_country = 'US') as us_rows,
    count(distinct customer_country) as distinct_customer_countries,
    count(*) as row_count,
    grouping(date_trunc('year', customer_signup_date)) as gid
from joined
group by cube (date_trunc('year', customer_signup_date))
order by th_count
