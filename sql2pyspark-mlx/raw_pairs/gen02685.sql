with remaining_keys as (
    -- Set difference on keys
    select promo_code
    from promotions
    except
    select promo_code
    from promotions
    where ends_on <= '2024-04-03'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from promotions s
    join remaining_keys k on s.promo_code = k.promo_code
),
matched_keys as (
    -- Set difference on keys
    select id, promo_code
    from kept_rows
    except distinct
    select id, promo_code
    from promotions
    where promo_code in ('BUNDLE', 'VIP')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from kept_rows s
    join matched_keys k on s.id = k.id and s.promo_code = k.promo_code
),
enriched as (
    -- Window metrics over several different partitions
    select *, dense_rank() over (order by discount_pct desc) as discount_pct_dense_rank
    from reconciled
    where month(starts_on) in (1, 3, 7)
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(discount_pct, 0) as discount_pct_rounded,
        coalesce(channel, 'DIRECT') as channel_clean
    from enriched
    where channel is not null
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        round(discount_pct_dense_rank / sum(discount_pct_dense_rank) over (partition by channel), 4) as discount_pct_dense_rank_share,
        rank() over (partition by promo_code order by discount_pct_rounded desc nulls first) as discount_pct_rounded_rank
    from filtered
),
filtered_promotions as (
    -- Basic cleansing and defaults
    select
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        channel_clean,
        discount_pct_dense_rank_share,
        datediff(starts_on, ends_on) / 7 as weeks_ends_on_to_starts_on
    from metrics
    where channel is not null
      and year(ends_on) >= 2025
)
-- Grouped metrics
select
    promo_code,
    case when discount_pct_dense_rank_share >= 0.8 then 'PREMIUM' else 'STANDARD' end as discount_pct_dense_rank_share_tier,
    round(sum(discount_pct) / nullif(count(case when channel = 'partner' then 1 end), 0), 2) as discount_pct_per_partner,
    sum(case when channel = 'mobile' then discount_pct else 0 end) as mobile_discount_pct,
    count(*) as event_count
from filtered_promotions
group by promo_code, case when discount_pct_dense_rank_share >= 0.8 then 'PREMIUM' else 'STANDARD' end
having discount_pct_per_partner >= 0;
