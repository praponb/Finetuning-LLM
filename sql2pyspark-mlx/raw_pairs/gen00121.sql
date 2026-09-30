with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        upper(promo_code) as promo_code_upper,
        case when channel = 'partner' then 1 else 0 end as is_partner,
        coalesce(channel, 'OTHER') as channel_clean
    from promotions
    where starts_on >= date '2023-05-06'
      and channel is not null
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        promo_code,
        starts_on,
        discount_pct,
        promo_code_upper,
        is_partner,
        channel_clean,
        date_trunc('YEAR', ends_on) as ends_on_period,
        coalesce(channel, 'N/A') as channel_filled
    from valid
    where promo_code in ('VIP', 'BUNDLE', 'SPRING10')
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from base
    qualify row_number() over (partition by promo_code order by starts_on, id) = 1
),
deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        promo_code,
        starts_on,
        discount_pct,
        promo_code_upper,
        channel_clean
    from (
        select *, rank() over (partition by promo_code order by ends_on_period) as latest_rank
        from latest
    ) t
    where latest_rank = 1
)
-- Aggregate with conditional metrics
select
    case when discount_pct >= 0.36 then 'LARGE' else 'SMALL' end as discount_pct_tier,
    stddev_samp(discount_pct) as discount_pct_stddev,
    percentile_approx(discount_pct, array(0.25, 0.5, 0.75)) as discount_pct_quartiles
from deduped
group by cube (case when discount_pct >= 0.36 then 'LARGE' else 'SMALL' end)
order by discount_pct_tier;
