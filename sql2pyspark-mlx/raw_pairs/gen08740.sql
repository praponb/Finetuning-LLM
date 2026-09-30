with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        explode_outer(coupon_codes) as element
    from carts
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        date_trunc('WEEK', created_at) as created_at_period,
        coalesce(element, 'UNKNOWN') as resolved_element
    from lines
    where element is not null
      and customer_id is null
),
joined as (
    -- Attach customers attributes
    select
        src.customer_id,
        src.created_at,
        src.element,
        p.segment
    from valid src
    left join customers p
        on src.customer_id = p.id
    where p.is_active = true
),
enriched as (
    -- Attach customers attributes
    select
        e.*,
        c.credit_limit,
        c.city,
        c.signup_date
    from joined e
    left join customers c
        on e.customer_id = c.id
)
-- Multi-level aggregation
select
    customer_id,
    case when credit_limit >= 12500 then 'HIGH' else 'LOW' end as credit_limit_tier,
    city,
    min(created_at) as first_seen,
    max(created_at) as ended_at,
    size(collect_set(city)) as num_cities,
    count(distinct segment) as distinct_segments,
    count(*) as row_count
from enriched
group by customer_id, case when credit_limit >= 12500 then 'HIGH' else 'LOW' end, city;
