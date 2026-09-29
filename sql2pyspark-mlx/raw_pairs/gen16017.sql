with filtered as (
    -- Keep recent, valid rows
    select *, floor(discount_pct) discount_pct_whole
    from promotions
    where starts_on >= date '2023-09-15'
      and channel is not null
),
matched as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from filtered a
    left join promotions pr
        on a.channel = pr.channel
       and a.starts_on >= pr.starts_on
       and a.starts_on < pr.ends_on + interval 14 days
),
with_window as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code_2,
        pr.discount_pct as promotion_discount_pct_2
    from matched a
    inner join promotions pr
        on coalesce(a.channel, 'web') = pr.channel
       and a.ends_on >= pr.starts_on
       and a.ends_on < pr.ends_on + interval '7' day
),
remaining_keys as (
    -- Set intersection on keys
    select id, promo_code
    from with_window
    intersect all
    select id, promo_code
    from promotions
    where channel in ('store', 'mobile')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_window s
    inner join remaining_keys k on s.id = k.id and s.promo_code = k.promo_code
)
select
    id,
    channel,
    promo_code,
    promotion_discount_pct,
    promo_id_2,
    promotion_promo_code_2,
    promotion_discount_pct_2
from verified
order by id;
