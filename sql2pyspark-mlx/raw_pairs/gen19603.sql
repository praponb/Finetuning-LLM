with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        attr_key,
        value
    from carts
    lateral view explode(attributes) t as attr_key, value
),
exploded as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        attr_key,
        code
    from flattened
    lateral view explode(coupon_codes) e as code
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        year(created_at) created_at_year,
        cast(created_at as date) created_at_day,
        coalesce(code, 'NONE') resolved_code
    from exploded
    where created_at > date '2025-03-02'
),
metrics as (
    -- Running and moving aggregates
    select
        created_at,
        attr_key,
        created_at_day,
        resolved_code,
        rank() over (partition by date_trunc('YEAR', created_at), attr_key order by created_at_year) created_at_year_rank,
        sum(created_at_year) over w_ordered created_at_year_sum_1h
    from cleaned
    window w_ordered as (order by cast(created_at as bigint) range between 3600 preceding and current row)
),
valid as (
    -- Basic cleansing and defaults
    select
        created_at,
        attr_key,
        created_at_day,
        created_at_year_rank,
        created_at_year_sum_1h,
        weekofyear(created_at_day) created_at_day_weekofyear,
        coalesce(attr_key, 'OTHER') attr_key_filled
    from metrics
    where attr_key is not null
)
select
    created_at,
    created_at_day,
    created_at_year_rank,
    created_at_year_sum_1h,
    created_at_day_weekofyear,
    attr_key_filled
from valid
order by created_at desc
