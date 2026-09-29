with lines as (
    -- Unnest the array into one row per element
    select
        base.id,
        base.coupon_codes,
        line.qty,
        line.price,
        line.promo
    from carts base
    lateral view outer explode(base.items) li as line
),
base as (
    -- Keep recent, valid rows
    select
        id,
        coupon_codes,
        qty,
        price,
        ceil(qty) as qty_whole
    from lines
    where promo in ('WELCOME5', 'VIP', 'SPRING10')
),
base_carts as (
    -- Basic cleansing and defaults
    select
        id,
        qty,
        qty_whole,
        qty_whole - price as qty_whole_minus_price,
        cast(qty_whole as string) as qty_whole_num
    from base
    where qty <> 3
),
base_carts_2 as (
    -- Basic cleansing and defaults
    select
        qty,
        qty_whole_minus_price,
        qty_whole_num,
        coalesce(qty, 0) as qty_filled
    from base_carts
    where qty is not null
),
metrics as (
    -- Ranking and gap features
    select
        qty,
        qty_whole_minus_price,
        dense_rank() over (order by qty desc) as qty_dense_rank,
        rank() over (order by qty_whole_minus_price asc nulls last) as qty_whole_minus_price_rank
    from base_carts_2
    where qty = 4
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, coalesce(qty_whole_minus_price, 0) as qty_whole_minus_price_filled
    from metrics
    where qty between 3 and 4
)
-- Aggregate with conditional metrics
select
    case when qty >= 5 then 'HIGH' else 'LOW' end as qty_tier,
    case when qty_whole_minus_price_filled >= 71 then 'HIGH' else 'LOW' end as qty_whole_minus_price_filled_tier,
    case when qty_whole_minus_price_rank >= 8 then 'PREMIUM' else 'STANDARD' end as qty_whole_minus_price_rank_tier,
    max(qty_dense_rank) as max_qty_dense_rank,
    round(avg(qty_dense_rank), 2) as avg_qty_dense_rank,
    round(avg(qty_whole_minus_price), 2) as avg_qty_whole_minus_price,
    round(avg(qty_whole_minus_price_filled), 2) as avg_qty_whole_minus_price_filled
from cleaned
group by case when qty >= 5 then 'HIGH' else 'LOW' end, case when qty_whole_minus_price_filled >= 71 then 'HIGH' else 'LOW' end, case when qty_whole_minus_price_rank >= 8 then 'PREMIUM' else 'STANDARD' end
order by max_qty_dense_rank desc
