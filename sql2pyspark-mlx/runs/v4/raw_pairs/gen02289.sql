with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        ceil(discount_pct) as discount_pct_whole,
        case when promo_code in ('BUNDLE', 'WELCOME5') then 'Tier 1' else 'Other' end as promo_code_group,
        coalesce(channel, 'NONE') as resolved_channel
    from promotions
    where ends_on > date '2023-09-13'
      and starts_on < '2024-04-01'
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        discount_pct_whole,
        promo_code_group,
        resolved_channel,
        lead(discount_pct, 1) over (partition by channel order by discount_pct_whole asc, id asc) as next_discount_pct,
        min(discount_pct_whole) over (partition by promo_code) as promo_code_min_discount_pct_whole,
        count(discount_pct_whole) over (partition by promo_code) as promo_code_count_discount_pct_whole,
        avg(discount_pct) over (partition by promo_code, channel order by discount_pct asc, id asc rows between 5 preceding and 1 preceding) as discount_pct_avg_5_prior,
        avg(discount_pct_whole) over (partition by channel order by ends_on asc, id asc rows between 5 preceding and 1 preceding) as discount_pct_whole_avg_5_prior
    from cleaned
),
matched as (
    -- Non-equi join on a validity window
    select
        src.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from enriched src
    left join promotions pr
        on src.channel <=> pr.channel
       and src.starts_on >= pr.starts_on
       and src.starts_on <= date_add(pr.ends_on, 3)
),
kept_keys as (
    -- Set intersection on keys
    select promo_code
    from matched
    intersect all
    select promo_code
    from promotions
    where promo_code = 'BUNDLE'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from matched s
    inner join kept_keys k on s.promo_code = k.promo_code
),
metrics as (
    -- Ranking and gap features
    select
        *,
        rank() over (partition by promo_code order by promo_code_count_discount_pct_whole desc) as promo_code_count_discount_pct_whole_rank,
        round(promo_code_count_discount_pct_whole / sum(promo_code_count_discount_pct_whole) over (partition by channel, promo_code), 4) as promo_code_count_discount_pct_whole_share
    from reconciled
)
select
    id,
    channel,
    starts_on,
    ends_on,
    discount_pct,
    discount_pct_whole,
    resolved_channel,
    promo_code_count_discount_pct_whole,
    discount_pct_avg_5_prior,
    case when channel rlike '^(store|web)$' then 'GROUP_A' else 'OTHER_ZONE' end as channel_zone
from metrics
order by id desc, ends_on asc
