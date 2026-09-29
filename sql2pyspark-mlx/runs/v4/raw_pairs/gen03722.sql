with valid as (
    -- Basic cleansing and defaults
    select
        *,
        upper(promo_code) as promo_code_upper,
        coalesce(channel, 'OTHER') as resolved_channel
    from promotions
    where starts_on > date '2023-05-21'
      and channel is not null
      and ends_on <= '2025-04-09'
),
matched_keys as (
    -- Set difference on keys
    select promo_code
    from valid
    except all
    select promo_code
    from promotions
    where promo_code = 'BUNDLE'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    inner join matched_keys k on s.promo_code = k.promo_code
),
kept_keys as (
    -- Set intersection on keys
    select channel, promo_code
    from reconciled
    intersect
    select channel, promo_code
    from promotions
    where discount_pct > 0.24
),
reconciled_promotions as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from reconciled s
    inner join kept_keys k on s.channel = k.channel and s.promo_code = k.promo_code
),
remaining_keys as (
    -- Set intersection on keys
    select id
    from reconciled_promotions
    intersect all
    select id
    from promotions
    where month(starts_on) in (5, 7, 9)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from reconciled_promotions s
    inner join remaining_keys k on s.id = k.id
)
select
    id,
    starts_on,
    discount_pct,
    promo_code_upper,
    resolved_channel
from verified
order by starts_on nulls last
