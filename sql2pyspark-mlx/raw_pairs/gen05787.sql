with deduped as (
    -- QUALIFY-style window filter
    select *
    from promotions
    qualify row_number() over (partition by promo_code, channel order by starts_on desc, id) = 1
),
matched_keys as (
    -- Set difference on keys
    select channel
    from deduped
    except all
    select channel
    from promotions
    where year(ends_on) = 2024
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from deduped s
    inner join matched_keys k on s.channel = k.channel
),
matched as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from verified s
    left join promotions pr
        on s.channel <=> pr.channel
       and s.starts_on between pr.starts_on and pr.ends_on
),
base as (
    -- Keep recent, valid rows
    select
        *,
        round(discount_pct / promotion_discount_pct, 2) as discount_pct_per_promotion_discount_pct,
        length(channel) as channel_length,
        coalesce(channel, 'OTHER') as resolved_channel
    from matched
    where starts_on >= date '2023-04-08'
      and starts_on >= '2024-06-29'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        channel,
        promo_code,
        discount_pct,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        discount_pct_per_promotion_discount_pct,
        channel_length,
        cume_dist() over (partition by date_trunc('WEEK', starts_on), channel order by discount_pct_per_promotion_discount_pct desc nulls last) as discount_pct_per_promotion_discount_pct_cume_dist,
        stddev_samp(discount_pct) over (partition by month(ends_on), channel) as ends_on_month_stddev_samp_discount_pct,
        avg(discount_pct_per_promotion_discount_pct) over (partition by promotion_promo_code, promo_code) as promotion_promo_code_avg_discount_pct_per_promotion_discount_pct
    from base
),
filtered as (
    -- Keep recent, valid rows
    select *, length(promo_code) as promo_code_length
    from with_windows
    where promotion_promo_code is not null
      and discount_pct_per_promotion_discount_pct <= 54
)
select
    channel,
    promo_code,
    discount_pct,
    promo_id,
    promotion_promo_code,
    promotion_discount_pct,
    channel_length,
    discount_pct_per_promotion_discount_pct_cume_dist,
    ends_on_month_stddev_samp_discount_pct,
    case when promotion_promo_code rlike '^(SPRING10|VIP)$' then 'EXTENDED' else 'PRIORITY' end as promotion_promo_code_zone
from filtered
order by promo_id desc nulls last
