with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        v
    from carts
    lateral view outer explode(attributes) e as k, v
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        v,
        sort_array(coupon_codes) as sorted_coupon_codes
    from exploded
    where created_at >= date '2025-03-01'
      and customer_id is not null
),
valid_carts as (
    -- Keep recent, valid rows
    select
        *,
        sort_array(sorted_coupon_codes) as sorted_sorted_coupon_codes,
        size(sorted_coupon_codes) as sorted_coupon_codes_count
    from valid
    where k is not null
),
joined as (
    -- Attach customers attributes
    select src.*, p.email
    from valid_carts src
    inner join customers p
        on src.customer_id = p.id
    where p.city != 'London'
),
with_dims as (
    -- Attach customers attributes
    select e.*, c.segment
    from joined e
    left join customers c
        on e.customer_id <=> c.id
),
enriched as (
    -- Attach customers attributes
    select
        a.*,
        p.city,
        p.country,
        p.segment as customer_segment
    from with_dims a
    left join customers p
        on a.customer_id = p.id
)
-- Multi-level aggregation
select
    date_trunc('day', created_at) as activity_day,
    date_trunc('hour', created_at) as hour_window,
    round(avg(sorted_coupon_codes_count), 2) as avg_sorted_coupon_codes_count,
    approx_percentile(sorted_coupon_codes_count, array(0.25, 0.5, 0.75)) as sorted_coupon_codes_count_quartiles,
    min(created_at) as started_at,
    min(created_at) as first_created_at,
    count_if(segment = 'Corporate') as corporate_events
from enriched
group by activity_day, hour_window
having round(avg(sorted_coupon_codes_count), 2) < 1000000
order by corporate_events desc nulls first;
