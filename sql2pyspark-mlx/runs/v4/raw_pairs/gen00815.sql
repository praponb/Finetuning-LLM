with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        pos,
        elem.price,
        elem.promo
    from carts
    lateral view posexplode(items) t as pos, elem
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        created_at,
        coupon_codes,
        pos,
        promo,
        array_join(coupon_codes, '|') coupon_codes_text,
        coalesce(promo, 'UNKNOWN') promo_filled
    from flattened
    where promo is not null
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by promo order by created_at, id, pos) as latest_rank
        from valid
    ) x
    where latest_rank = 1
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        pos - latest_rank pos_minus_latest_rank,
        cast(created_at as date) created_at_day
    from deduped
    where array_contains(coupon_codes, 'VIP')
)
-- Multi-level aggregation
select
    date_trunc('day', created_at) as activity_day,
    promo,
    sum(pos) filter (where promo = 'VIP') as vip_total_pos,
    round(avg(pos), 2) as avg_pos,
    max(pos) filter (where promo = 'WELCOME5') as welcome5_max_pos,
    avg(pos) as avg_pos_2
from filtered
group by 1, 2
order by welcome5_max_pos desc nulls last;
