with attributed as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from promotions e
    left join promotions pr
        on e.channel = pr.channel
       and e.ends_on >= pr.starts_on
       and e.ends_on < pr.ends_on + interval 14 days
),
with_window as (
    -- Non-equi join on a validity window
    select
        src.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code_2,
        pr.discount_pct as promotion_discount_pct_2
    from attributed src
    left join promotions pr
        on src.channel = pr.channel
       and src.starts_on between pr.starts_on and pr.ends_on
),
matched as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id_3,
        pr.promo_code as promotion_promo_code_3,
        pr.discount_pct as promotion_discount_pct_3
    from with_window e
    left join promotions pr
        on e.channel = pr.channel
       and e.starts_on between pr.starts_on and pr.ends_on
),
with_window_promotions as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id_4,
        pr.promo_code as promotion_promo_code_4,
        pr.discount_pct as promotion_discount_pct_4
    from matched a
    left join promotions pr
        on a.channel <=> pr.channel
       and a.starts_on >= pr.starts_on
       and a.starts_on < pr.ends_on + interval 7 days
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, promo_id_2 * promotion_discount_pct as promo_id_2_times_promotion_discount_pct
    from with_window_promotions
    where ends_on > date '2024-05-15'
      and channel is not null
)
-- Grouped metrics
select
    date_trunc('month', starts_on) as activity_month,
    date_trunc('quarter', starts_on) as quarter_start,
    round(sum(discount_pct) / nullif(count(case when promotion_promo_code = 'WELCOME5' then 1 end), 0), 2) as discount_pct_per_welcome5,
    size(collect_set(promotion_promo_code)) as num_promotion_promo_codes,
    sort_array(collect_set(promotion_promo_code)) as promotion_promo_code_list,
    approx_percentile(promotion_discount_pct, array(0.25, 0.5, 0.75)) as promotion_discount_pct_quartiles,
    count_if(promotion_promo_code_3 = 'BUNDLE') as bundle_events,
    dense_rank() over (partition by date_trunc('month', starts_on) order by count_if(promotion_promo_code_3 = 'BUNDLE') desc) as bundle_events_dense_rank
from valid
group by activity_month, quarter_start
order by bundle_events desc
