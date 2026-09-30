with valid as (
    select
        *,
        case when discount_pct >= 0.26 then 'High' when discount_pct >= 0.14 then 'Medium' else 'Low' end as discount_pct_level,
        coalesce(channel, 'NONE') as channel_filled
    from promotions
    where channel is not null
      and channel is distinct from 'partner'
),
enriched as (
    select
        *,
        ntile(4) over w_running as discount_pct_bucket,
        avg(discount_pct) over w_running_2 as discount_pct_avg_5,
        first_value(channel) ignore nulls over (partition by channel, promo_code order by ends_on, id rows between unbounded preceding and current row) as first_known_channel
    from valid
    window w_running as (order by discount_pct, id), w_running_2 as (partition by promo_code order by starts_on, id rows between 5 preceding and current row)
),
attributed as (
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from enriched e
    inner join promotions pr
        on e.channel = pr.channel
       and e.starts_on >= pr.starts_on
       and e.starts_on < pr.ends_on + interval '7' day
),
attributed_promotions as (
    select
        s.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code_2,
        pr.discount_pct as promotion_discount_pct_2
    from attributed s
    inner join promotions pr
        on coalesce(s.channel, 'store') = pr.channel
       and s.ends_on between pr.starts_on and pr.ends_on
),
matched as (
    select
        s.*,
        pr.id as promo_id_3,
        pr.promo_code as promotion_promo_code_3,
        pr.discount_pct as promotion_discount_pct_3
    from attributed_promotions s
    left join promotions pr
        on coalesce(s.channel, 'store') = pr.channel
       and s.ends_on >= pr.starts_on
       and s.ends_on < pr.ends_on + interval '30' day
),
totals as (
    select
        case when promotion_discount_pct_3 >= 0.26 then 'PREMIUM' else 'STANDARD' end as promotion_discount_pct_3_tier,
        promotion_promo_code_2,
        promotion_promo_code_3,
        round(sum(promo_id_3) / nullif(count(case when promotion_promo_code_3 = 'SPRING10' then 1 end), 0), 2) as promo_id_3_per_spring10,
        approx_percentile(promo_id_2, 0.9) as promo_id_2_p90,
        sort_array(collect_set(promo_code)) as promo_code_list,
        sum(promotion_discount_pct_3) filter (where promotion_promo_code = 'WELCOME5') as welcome5_total_promotion_discount_pct_3
    from matched
    group by grouping sets ((case when promotion_discount_pct_3 >= 0.26 then 'PREMIUM' else 'STANDARD' end, promotion_promo_code_2, promotion_promo_code_3), (case when promotion_discount_pct_3 >= 0.26 then 'PREMIUM' else 'STANDARD' end, promotion_promo_code_2))
)
select
    promotion_discount_pct_3_tier,
    promotion_promo_code_2,
    promotion_promo_code_3,
    promo_id_3_per_spring10,
    promo_id_2_p90
from totals
