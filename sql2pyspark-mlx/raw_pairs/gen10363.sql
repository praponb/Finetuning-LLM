with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        line.qty,
        line.price,
        line.promo
    from carts
    lateral view explode(items) e as line
),
base as (
    -- Keep recent, valid rows
    select
        id,
        created_at,
        qty,
        promo,
        hour(created_at) as created_at_hour,
        datediff(current_date(), created_at) / 365 as years_since_created_at
    from flattened
    where promo is not null
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        qty / nullif(created_at_hour, 0) as qty_per_created_at_hour,
        lower(promo) as promo_lower
    from base
    where created_at >= date '2025-03-01'
      and created_at_hour <= 27
),
scored as (
    -- Running and moving aggregates
    select
        *,
        avg(years_since_created_at) over (partition by month(created_at), promo order by cast(created_at as bigint) range between 3600 preceding and current row) as years_since_created_at_avg_1h,
        max(qty) over (partition by month(created_at), promo) as created_at_month_max_qty,
        avg(qty_per_created_at_hour) over (partition by promo) as promo_avg_qty_per_created_at_hour
    from filtered
    where created_at < '2024-03-11'
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        promo,
        qty_per_created_at_hour,
        years_since_created_at
    from scored
) src
unpivot (
    reading for measure in (qty_per_created_at_hour, years_since_created_at)
)
