with matched as (
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from promotions e
    left join promotions pr
        on coalesce(e.channel, 'store') = pr.channel
       and e.starts_on between pr.starts_on and pr.ends_on
),
scored as (
    select
        *,
        ntile(10) over (partition by promo_code order by promotion_discount_pct desc, id asc, promo_id asc) as promotion_discount_pct_bucket,
        rank() over (partition by promotion_promo_code order by discount_pct desc) as discount_pct_rank,
        round(discount_pct / sum(discount_pct) over (partition by promotion_promo_code, promo_code), 4) as discount_pct_share,
        lead(promotion_discount_pct, 2) over (order by starts_on asc, id asc, promo_id asc) as next_promotion_discount_pct
    from matched
),
scored_promotions as (
    select
        id,
        promo_id,
        discount_pct_rank,
        next_promotion_discount_pct,
        round(discount_pct_rank / sum(discount_pct_rank) over (partition by promo_code), 4) as discount_pct_rank_share,
        lead(discount_pct_rank, 1) over (partition by promo_code order by starts_on asc, id asc, promo_id asc) as next_discount_pct_rank,
        last_value(promotion_promo_code) ignore nulls over w_running as last_known_promotion_promo_code
    from scored
    where next_promotion_discount_pct < 0.25
    window w_running as (order by next_promotion_discount_pct asc, id asc, promo_id asc rows between unbounded preceding and current row)
),
with_windows as (
    select
        *,
        last_value(discount_pct_rank) ignore nulls over w_running as last_known_discount_pct_rank,
        first_value(promo_id) ignore nulls over (order by next_promotion_discount_pct asc, id asc, promo_id asc rows between unbounded preceding and current row) as first_known_promo_id
    from scored_promotions
    window w_running as (order by promo_id asc, id asc, promo_id asc rows between unbounded preceding and current row)
),
cleaned as (
    select
        *,
        coalesce(next_promotion_discount_pct, 0) as next_promotion_discount_pct_filled,
        round(next_discount_pct_rank / last_known_discount_pct_rank, 2) as next_discount_pct_rank_per_last_known_discount_pct_rank
    from with_windows
    where promo_id is not null
)
select
    case when next_discount_pct_rank >= 7 then 'LARGE' else 'SMALL' end as next_discount_pct_rank_tier,
    percentile_approx(next_promotion_discount_pct_filled, array(0.25, 0.5, 0.75)) as next_promotion_discount_pct_filled_quartiles,
    approx_percentile(discount_pct_rank, 0.5) as discount_pct_rank_p50,
    count(distinct id) as distinct_ids,
    count(distinct id) as distinct_ids_2,
    max(promo_id) as max_promo_id,
    max(last_known_discount_pct_rank) as max_last_known_discount_pct_rank
from cleaned
group by case when next_discount_pct_rank >= 7 then 'LARGE' else 'SMALL' end
