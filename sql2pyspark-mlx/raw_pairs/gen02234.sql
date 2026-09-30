with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        tag
    from carts
    lateral view explode(coupon_codes) t as tag
),
with_dims as (
    -- Attach customers attributes
    select
        s.*,
        p.country,
        p.signup_date,
        p.age
    from flattened s
    left join customers p
        on s.customer_id = p.id
),
metrics as (
    -- Ranking and gap features
    select *, sum(age) over w_running as country_sum_age
    from with_dims
    window w_running as (partition by country)
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        created_at,
        signup_date,
        datediff(current_date(), created_at) / 7 as weeks_since_created_at
    from metrics
    where customer_id is not null
),
grouped as (
    -- Grouped metrics
    select
        date_trunc('week', signup_date) as week_start,
        customer_id,
        max(created_at) as last_seen,
        percentile_approx(weeks_since_created_at, 0.5) as weeks_since_created_at_p50
    from cleaned
    group by date_trunc('week', signup_date), customer_id
    having percentile_approx(weeks_since_created_at, 0.5) >= 0
)
select
    week_start,
    customer_id,
    last_seen
from grouped
order by customer_id desc nulls last
