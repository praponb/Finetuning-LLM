with valid as (
    select
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        trim(promo_code) as clean_promo_code
    from promotions
    where promo_code in ('SPRING10', 'BUNDLE')
),
attributed as (
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from valid e
    left join promotions pr
        on e.channel = pr.channel
       and e.starts_on >= pr.starts_on
       and e.starts_on < pr.ends_on + interval '30' day
),
remaining_keys as (
    select channel, id
    from attributed
    intersect
    select channel, id
    from promotions
    where discount_pct <= 0.15
),
kept_rows as (
    select s.*
    from attributed s
    join remaining_keys k on s.channel = k.channel and s.id = k.id
),
with_window as (
    select
        src.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code_2,
        pr.discount_pct as promotion_discount_pct_2
    from kept_rows src
    left join promotions pr
        on coalesce(src.channel, 'web') = pr.channel
       and src.starts_on >= pr.starts_on
       and src.starts_on < pr.ends_on + interval '14' day
)
select
    promotion_promo_code,
    count(*) as row_count,
    sum(case when promo_code = 'VIP' then promo_id_2 else 0 end) as vip_promo_id_2,
    percentile_approx(promotion_discount_pct, 0.9) as promotion_discount_pct_p90
from with_window
group by promotion_promo_code
