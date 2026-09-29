with valid as (
    -- Filter the raw rows and derive helper columns
    select *, case when channel in ('store', 'partner') then 'Group A' else 'Other' end channel_group
    from promotions
    where ends_on >= to_date('2023-09-24')
      and channel is not null
),
matched as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from valid e
    left join promotions pr
        on coalesce(e.channel, 'store') = pr.channel
       and e.starts_on >= pr.starts_on
       and e.starts_on < pr.ends_on + interval '30' day
),
remaining_keys as (
    -- Set intersection on keys
    select channel, promo_code
    from matched
    intersect
    select channel, promo_code
    from promotions
    where starts_on <= '2025-03-05'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from matched s
    inner join remaining_keys k on s.channel = k.channel and s.promo_code = k.promo_code
),
matched_promotions as (
    -- Non-equi join on a validity window
    select
        src.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code_2,
        pr.discount_pct as promotion_discount_pct_2
    from kept_rows src
    left join promotions pr
        on src.channel <=> pr.channel
       and src.ends_on >= pr.starts_on
       and src.ends_on < pr.ends_on + interval '7' day
),
with_window as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id_3,
        pr.promo_code as promotion_promo_code_3,
        pr.discount_pct as promotion_discount_pct_3
    from matched_promotions s
    left join promotions pr
        on s.channel = pr.channel
       and s.ends_on >= pr.starts_on
       and s.ends_on <= date_add(pr.ends_on, 3)
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        promo_id_2,
        promotion_promo_code_2,
        promotion_discount_pct_2,
        promo_id_3,
        promotion_promo_code_3,
        length(promotion_promo_code_2) promotion_promo_code_2_length
    from with_window
    where promotion_promo_code_2 <> 'VIP'
)
select
    id,
    channel,
    promo_code,
    promo_id,
    promotion_promo_code,
    promotion_discount_pct,
    promotion_discount_pct_2,
    promo_id_3,
    promotion_promo_code_3
from base
order by promotion_promo_code desc, promo_code
