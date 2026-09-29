with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        v
    from carts
    lateral view outer explode(attributes) li as k, v
),
base as (
    -- Keep recent, valid rows
    select
        *,
        sort_array(coupon_codes) as sorted_coupon_codes,
        coalesce(k, 'N/A') as resolved_k
    from lines
    where size(coupon_codes) >= 2
),
enriched as (
    -- Attach customers attributes
    select
        a.*,
        p.signup_date,
        p.country,
        p.email
    from base a
    inner join customers p
        on a.customer_id = p.id
),
enriched_carts as (
    -- Attach customers attributes
    select
        s.customer_id,
        s.created_at,
        s.k,
        s.sorted_coupon_codes,
        s.resolved_k,
        s.country,
        b.segment,
        b.name,
        b.email as customer_email
    from enriched s
    left join customers b
        on s.customer_id = b.id
),
joined as (
    -- Attach customers attributes
    select
        src.customer_id,
        src.created_at,
        src.k,
        src.country,
        src.segment,
        src.name,
        b.email,
        b.credit_limit,
        b.city
    from enriched_carts src
    left join customers b
        on src.customer_id = b.id
)
-- Aggregate with conditional metrics
select
    country,
    min(created_at) as first_created_at,
    count(distinct country) as distinct_countries
from joined
group by country
order by distinct_countries nulls last;
