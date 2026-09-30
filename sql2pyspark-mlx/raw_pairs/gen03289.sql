with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        device,
        started_at,
        country,
        lower(country) as country_lower
    from sessions
    where country = 'JP'
),
scored as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        device,
        country,
        country_lower,
        first_value(country) ignore nulls over (partition by country order by started_at, id rows between unbounded preceding and current row) as first_known_country,
        last_value(country, true) over (partition by country order by started_at, id rows between unbounded preceding and current row) as last_known_country
    from cleaned
    where device <> 'desktop'
),
joined as (
    -- Attach customers attributes
    select
        s.id,
        s.customer_id,
        s.country,
        s.last_known_country,
        p.country as customer_country
    from scored s
    left join customers p
        on s.customer_id <=> p.id
),
with_dims as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.last_known_country,
        e.customer_country,
        d.credit_limit,
        d.city
    from joined e
    left join customers d
        on e.customer_id = d.id
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when customer_country = 'SG' then 1 else 0 end as is_sg,
        length(customer_country) as customer_country_length
    from with_dims
    where city is not null
)
select
    id,
    customer_id,
    last_known_country,
    customer_country,
    credit_limit,
    customer_country_length
from base
