with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, concat_ws(',', channel, promo_code) channel_promo_code_key
    from promotions
    where starts_on >= to_date('2023-10-13')
      and channel is not null
      and discount_pct <= 0.28
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        sum(discount_pct) over (partition by channel order by ends_on, id rows between unbounded preceding and current row) running_discount_pct,
        ntile(3) over (partition by promo_code order by discount_pct, id) discount_pct_bucket,
        last_value(channel, true) over (partition by promo_code order by discount_pct, id rows between unbounded preceding and current row) last_known_channel
    from filtered
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        rank() over (partition by promo_code, channel order by discount_pct desc) discount_pct_rank,
        cume_dist() over (partition by promo_code, channel order by discount_pct_bucket desc nulls last) discount_pct_bucket_cume_dist
    from metrics
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        round(running_discount_pct / sum(running_discount_pct) over (partition by channel), 4) running_discount_pct_share,
        lead(discount_pct_bucket, 1, 0) over (partition by promo_code, channel order by discount_pct, id) next_discount_pct_bucket,
        ntile(10) over (partition by channel order by discount_pct_bucket, id) discount_pct_bucket_bucket
    from enriched
),
matched as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from with_windows s
    left join promotions pr
        on s.channel <=> pr.channel
       and s.ends_on between pr.starts_on and pr.ends_on
)
-- Grouped metrics
select
    date_trunc('week', ends_on) as week_start,
    round(sum(running_discount_pct) / nullif(count(case when promo_code = 'SPRING10' then 1 end), 0), 2) as running_discount_pct_per_spring10,
    sum(case when promotion_promo_code = 'SPRING10' then running_discount_pct else 0 end) as spring10_running_discount_pct,
    percentile_approx(discount_pct_bucket_cume_dist, array(0.25, 0.5, 0.75)) as discount_pct_bucket_cume_dist_quartiles,
    round(sum(promotion_discount_pct) / nullif(count(case when channel = 'partner' then 1 end), 0), 2) as promotion_discount_pct_per_partner,
    count(distinct id) as distinct_ids,
    percentile_approx(discount_pct_bucket_bucket, 0.5) as discount_pct_bucket_bucket_p50
from matched
group by date_trunc('week', ends_on);
