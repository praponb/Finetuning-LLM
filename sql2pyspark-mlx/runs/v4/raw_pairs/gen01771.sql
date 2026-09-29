with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        explode(coupon_codes) as element
    from carts
),
valid as (
    -- Keep recent, valid rows
    select *, cast(created_at as date) as created_at_day
    from exploded
    where element is not null
      and element in ('SPRING10', 'VIP')
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        created_at,
        element,
        created_at_day,
        initcap(trim(element)) as element_title
    from valid
    where created_at >= date '2025-03-02'
),
enriched as (
    -- Attach customers attributes
    select a.*, p.signup_date
    from filtered a
    left join customers p
        on a.customer_id = p.id
),
filtered_carts as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        created_at,
        element,
        created_at_day,
        element_title,
        signup_date,
        datediff(current_date(), created_at_day) as days_since_created_at_day
    from enriched
    where created_at < '2023-05-09'
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over w_ordered as days_since_created_at_day_dense_rank,
        dense_rank() over (order by days_since_created_at_day nulls last) as days_since_created_at_day_dense_rank_2
    from filtered_carts
    window w_ordered as (partition by element, customer_id order by days_since_created_at_day)
),
metrics_carts as (
    -- Running and moving aggregates
    select
        *,
        rank() over (partition by element order by days_since_created_at_day) as days_since_created_at_day_rank,
        min(days_since_created_at_day_dense_rank_2) over (partition by element) as element_min_days_since_created_at_day_dense_rank_2,
        count(days_since_created_at_day) over (order by cast(created_at as bigint) range between 900 preceding and current row) as days_since_created_at_day_count_15m
    from metrics
)
-- Multi-level aggregation
select
    case when days_since_created_at_day_dense_rank >= 7 then 'PREMIUM' else 'STANDARD' end as days_since_created_at_day_dense_rank_tier,
    case when days_since_created_at_day_count_15m >= 18 then 'HIGH' else 'LOW' end as days_since_created_at_day_count_15m_tier,
    count_if(element = 'SPRING10') as spring10_events,
    count(*) filter (where element = 'BUNDLE') as bundle_rows,
    count(*) filter (where element = 'BUNDLE') as bundle_rows_2
from metrics_carts
group by days_since_created_at_day_dense_rank_tier, days_since_created_at_day_count_15m_tier
