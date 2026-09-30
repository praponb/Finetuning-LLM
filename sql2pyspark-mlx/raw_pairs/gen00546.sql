with remaining_keys as (
    -- Set intersection on keys
    select channel, id
    from promotions
    intersect
    select channel, id
    from promotions
    where ends_on <= '2023-11-01'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from promotions s
    inner join remaining_keys k on s.channel = k.channel and s.id = k.id
),
with_window as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from verified e
    inner join promotions pr
        on e.channel = pr.channel
       and e.starts_on >= pr.starts_on
       and e.starts_on < pr.ends_on + interval '30' day
),
enriched as (
    -- Ranking and gap features
    select
        *,
        round(promotion_discount_pct / sum(promotion_discount_pct) over (partition by year(starts_on), promotion_promo_code), 4) as promotion_discount_pct_share,
        rank() over (order by discount_pct asc) as discount_pct_rank,
        rank() over (order by promotion_discount_pct desc nulls first) as promotion_discount_pct_rank
    from with_window
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        round(promotion_discount_pct / sum(promotion_discount_pct) over (partition by promotion_promo_code), 4) as promotion_discount_pct_share_2,
        stddev_samp(promotion_discount_pct_share) over (partition by promotion_promo_code) as promotion_promo_code_stddev_samp_promotion_discount_pct_share
    from enriched
    where promo_code = 'SPRING10'
),
with_window_promotions as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code_2,
        pr.discount_pct as promotion_discount_pct_2
    from with_windows a
    left join promotions pr
        on a.channel <=> pr.channel
       and a.ends_on >= pr.starts_on
       and a.ends_on < pr.ends_on + interval 30 days
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        promo_code,
        starts_on,
        discount_pct,
        promotion_promo_code,
        promotion_discount_pct,
        promotion_discount_pct_share,
        discount_pct_rank,
        promotion_discount_pct_rank,
        promotion_discount_pct_share_2,
        promotion_promo_code_stddev_samp_promotion_discount_pct_share,
        promo_id_2,
        promotion_discount_pct_2,
        round(discount_pct_rank * 1.15, 2) as discount_pct_rank_adjusted
    from with_window_promotions
    where discount_pct_rank = 8
)
select
    id,
    promo_code,
    starts_on,
    promotion_promo_code,
    promotion_discount_pct_rank,
    promotion_discount_pct_share_2,
    promo_id_2,
    promotion_discount_pct_2,
    discount_pct_rank_adjusted
from base
