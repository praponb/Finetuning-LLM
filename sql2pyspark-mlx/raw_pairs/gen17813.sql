with matched as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from promotions s
    left join promotions pr
        on coalesce(s.channel, 'web') = pr.channel
       and s.starts_on between pr.starts_on and pr.ends_on
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by channel order by promo_id nulls last, id, promo_id) as latest_rank
    from matched
    qualify latest_rank = 1
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by channel order by ends_on desc, id, promo_id) as latest_rank_2
    from latest
    qualify latest_rank_2 = 1
),
with_window as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code_2,
        pr.discount_pct as promotion_discount_pct_2
    from deduped s
    left join promotions pr
        on s.channel <=> pr.channel
       and s.ends_on >= pr.starts_on
       and s.ends_on < pr.ends_on + interval '14' day
),
attributed as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id_3,
        pr.promo_code as promotion_promo_code_3,
        pr.discount_pct as promotion_discount_pct_3
    from with_window s
    inner join promotions pr
        on coalesce(s.channel, 'web') = pr.channel
       and s.starts_on >= pr.starts_on
       and s.starts_on <= date_add(pr.ends_on, 10)
)
-- Window metrics over several different partitions
select
    id,
    promo_code,
    ends_on,
    discount_pct,
    promo_id,
    promotion_promo_code,
    promotion_discount_pct,
    latest_rank,
    latest_rank_2,
    promo_id_2,
    promotion_promo_code_2,
    promo_id_3,
    promotion_discount_pct_3,
    round(promo_id / sum(promo_id) over (partition by promotion_promo_code_2), 4) as promo_id_share,
    row_number() over (partition by date_trunc('week', starts_on), channel order by promotion_discount_pct desc, id, promo_id, promo_id_2, promo_id_3) as event_seq,
    lag(discount_pct, 2) over (partition by promotion_promo_code, promotion_promo_code_2 order by promotion_discount_pct_3, id, promo_id, promo_id_2, promo_id_3) as prev_discount_pct,
    round(promo_id / sum(promo_id) over (partition by promotion_promo_code_2), 4) as promo_id_share_2,
    row_number() over w_hist as seq_no
from attributed
where discount_pct < 0.16
window w_hist as (order by latest_rank_2 desc, id, promo_id, promo_id_2, promo_id_3)
