with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        explode_outer(coupon_codes) as code
    from carts
)
-- Aggregate with conditional metrics
select
    customer_id,
    min(created_at) as first_seen,
    count(*) as cnt
from flattened
group by customer_id
order by cnt desc nulls last;
