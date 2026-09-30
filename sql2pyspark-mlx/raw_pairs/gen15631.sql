with matched as (
    select
        s.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from promotions s
    left join promotions pr
        on s.channel = pr.channel
       and s.starts_on >= pr.starts_on
       and s.starts_on < pr.ends_on + interval 30 days
),
metrics as (
    select
        id,
        promo_code,
        discount_pct,
        promo_id,
        avg(promotion_discount_pct) over (order by promotion_discount_pct asc, id asc, promo_id asc rows between 6 preceding and current row) promotion_discount_pct_avg_6,
        last_value(promotion_promo_code, true) over (order by promotion_discount_pct asc, id asc, promo_id asc rows between unbounded preceding and current row) last_known_promotion_promo_code
    from matched
),
latest as (
    select *, rank() over (partition by promo_code order by discount_pct asc) as latest_rank
    from metrics
    qualify latest_rank = 1
),
latest_promotions as (
    select
        id,
        discount_pct,
        promo_id,
        promotion_discount_pct_avg_6,
        last_known_promotion_promo_code,
        latest_rank
    from latest
    qualify rank() over (partition by promo_code order by latest_rank desc) = 1
)
select
    case when promotion_discount_pct_avg_6 >= 0 then 'LARGE' else 'SMALL' end as promotion_discount_pct_avg_6_tier,
    sum(discount_pct) as total_discount_pct,
    count(*) as event_count,
    count(distinct id) as distinct_ids,
    count(*) as num_rows,
    avg(discount_pct) as avg_discount_pct
from latest_promotions
group by case when promotion_discount_pct_avg_6 >= 0 then 'LARGE' else 'SMALL' end
having num_rows > 1
order by event_count asc;
