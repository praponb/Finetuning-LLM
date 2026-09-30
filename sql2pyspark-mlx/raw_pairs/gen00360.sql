with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(discount_pct * 1.15, 2) as adjusted_discount_pct,
        coalesce(channel, 'DIRECT') as channel_filled
    from promotions
    where channel is not null
      and ends_on >= '2025-03-07'
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        adjusted_discount_pct,
        channel_filled,
        adjusted_discount_pct + discount_pct as adjusted_discount_pct_plus_discount_pct,
        coalesce(channel, 'UNKNOWN') as channel_clean
    from cleaned
    where channel is not distinct from 'partner'
      and starts_on > to_date('2023-07-01')
),
valid_promotions as (
    -- Keep recent, valid rows
    select
        *,
        cast(adjusted_discount_pct as int) as adjusted_discount_pct_int,
        coalesce(channel, 'OTHER') as channel_clean_2
    from valid
    where ends_on > date '2023-08-09'
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        promo_code,
        ends_on,
        discount_pct,
        adjusted_discount_pct,
        channel_filled,
        adjusted_discount_pct_plus_discount_pct,
        channel_clean,
        adjusted_discount_pct_int,
        channel_clean_2,
        floor(adjusted_discount_pct_plus_discount_pct) as adjusted_discount_pct_plus_discount_pct_whole
    from valid_promotions
    where adjusted_discount_pct_plus_discount_pct > 32
),
base_promotions as (
    -- Filter the raw rows and derive helper columns
    select *, date_trunc('month', ends_on) as ends_on_period
    from base
    where ends_on >= date '2023-10-15'
)
-- Grouped metrics
select
    case when discount_pct >= 0.29 then 'HIGH' else 'LOW' end as discount_pct_tier,
    case when adjusted_discount_pct_plus_discount_pct >= 76 then 'PREMIUM' else 'STANDARD' end as adjusted_discount_pct_plus_discount_pct_tier,
    approx_percentile(adjusted_discount_pct_plus_discount_pct_whole, array(0.25, 0.5, 0.75)) as adjusted_discount_pct_plus_discount_pct_whole_quartiles,
    approx_percentile(adjusted_discount_pct_int, 0.5) as adjusted_discount_pct_int_p50,
    rank() over (partition by case when discount_pct >= 0.29 then 'HIGH' else 'LOW' end order by approx_percentile(adjusted_discount_pct_int, 0.5) desc) as adjusted_discount_pct_int_p50_rank
from base_promotions
group by case when discount_pct >= 0.29 then 'HIGH' else 'LOW' end, case when adjusted_discount_pct_plus_discount_pct >= 76 then 'PREMIUM' else 'STANDARD' end
order by discount_pct_tier desc
