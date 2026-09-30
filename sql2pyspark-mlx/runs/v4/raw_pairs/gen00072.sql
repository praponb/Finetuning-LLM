with matched_keys as (
    -- Set difference on keys
    select promo_code, channel
    from promotions
    except
    select promo_code, channel
    from promotions
    where promo_code != 'WELCOME5'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from promotions s
    join matched_keys k on s.promo_code = k.promo_code and s.channel = k.channel
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
        round(discount_pct / sum(discount_pct) over (partition by promo_code), 4) as discount_pct_share,
        dense_rank() over w_part as discount_pct_dense_rank,
        rank() over (partition by promo_code order by discount_pct desc) as discount_pct_rank
    from verified
    window w_part as (partition by promo_code order by discount_pct)
),
matched as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from enriched s
    left join promotions pr
        on s.channel <=> pr.channel
       and s.ends_on >= pr.starts_on
       and s.ends_on <= date_add(pr.ends_on, 10)
),
metrics as (
    -- Ranking and gap features
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        discount_pct_share,
        discount_pct_dense_rank,
        discount_pct_rank,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        dense_rank() over (partition by promotion_promo_code order by discount_pct_dense_rank desc nulls last) as discount_pct_dense_rank_dense_rank,
        percent_rank() over (order by discount_pct_rank) as discount_pct_rank_percent_rank
    from matched
),
daily as (
    -- Aggregate with conditional metrics
    select
        case when promo_id >= 9 then 'PREMIUM' else 'STANDARD' end as promo_id_tier,
        avg(discount_pct_rank) as avg_discount_pct_rank,
        round(sum(promo_id) / nullif(count(case when channel = 'mobile' then 1 end), 0), 2) as promo_id_per_mobile,
        min(discount_pct_rank_percent_rank) as min_discount_pct_rank_percent_rank
    from metrics
    group by 1
    having avg(discount_pct_rank) < 0
)
select
    promo_id_tier,
    avg_discount_pct_rank,
    min_discount_pct_rank_percent_rank
from daily;
