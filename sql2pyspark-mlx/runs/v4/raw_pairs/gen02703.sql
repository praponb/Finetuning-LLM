with filtered as (
    -- Keep recent, valid rows
    select
        id,
        channel,
        promo_code,
        ends_on,
        round(discount_pct * 0.85, 2) adjusted_discount_pct,
        coalesce(channel, 'N/A') channel_clean
    from promotions
    where channel = 'partner'
),
latest as (
    -- QUALIFY-style window filter
    select *
    from filtered
    qualify rank() over (partition by promo_code, channel order by ends_on) = 1
),
with_window as (
    -- Non-equi join on a validity window
    select
        src.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct
    from latest src
    left join promotions pr
        on src.channel <=> pr.channel
       and src.ends_on between pr.starts_on and pr.ends_on
),
latest_promotions as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by channel order by discount_pct nulls first, id, promo_id) as rank_in_group
    from with_window
    qualify rank_in_group = 1
)
-- Multi-level aggregation
select
    case when adjusted_discount_pct >= 89 then 'HIGH' else 'LOW' end as adjusted_discount_pct_tier,
    channel,
    min(discount_pct) as min_discount_pct,
    percentile_approx(promo_id, array(0.25, 0.5, 0.75)) as promo_id_quartiles
from latest_promotions
group by case when adjusted_discount_pct >= 89 then 'HIGH' else 'LOW' end, channel
