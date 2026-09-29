with lines as (
    select
        id,
        plan,
        currency,
        val
    from price_tiers
    lateral view explode(tier_limits) li as val
),
valid as (
    select *, lower(plan) as plan_lower
    from lines
    where val <= 29
),
valid_price_tiers as (
    select *, case when currency = 'EUR' then 1 else 0 end as is_eur
    from valid
    where val >= 60
),
scored as (
    select
        id,
        val,
        plan_lower,
        cume_dist() over w_hist as is_eur_cume_dist,
        stddev_samp(val) over w_running as currency_stddev_samp_val,
        dense_rank() over w_hist_2 as is_eur_dense_rank,
        avg(is_eur) over w_hist_3 as currency_avg_is_eur
    from valid_price_tiers
    where currency in ('EUR', 'THB', 'USD')
    window w_hist as (partition by plan order by is_eur desc), w_running as (partition by currency, plan), w_hist_2 as (partition by plan, currency order by is_eur desc nulls first), w_hist_3 as (partition by currency)
),
with_windows as (
    select *, cume_dist() over (order by is_eur_dense_rank desc nulls first) as is_eur_dense_rank_cume_dist
    from scored
),
grouped as (
    select
        case when is_eur_dense_rank_cume_dist >= 0.87 then 'HIGH' else 'LOW' end as is_eur_dense_rank_cume_dist_tier,
        case when is_eur_dense_rank >= 7 then 'HIGH' else 'LOW' end as is_eur_dense_rank_tier,
        case when is_eur_cume_dist >= 0.69 then 'HIGH' else 'LOW' end as is_eur_cume_dist_tier,
        count(*) as cnt,
        count(*) as cnt_2,
        percentile_approx(is_eur_dense_rank, array(0.25, 0.5, 0.75)) as is_eur_dense_rank_quartiles,
        approx_percentile(is_eur_dense_rank_cume_dist, array(0.25, 0.5, 0.75)) as is_eur_dense_rank_cume_dist_quartiles
    from with_windows
    group by is_eur_dense_rank_cume_dist_tier, is_eur_dense_rank_tier, is_eur_cume_dist_tier
)
select
    is_eur_dense_rank_cume_dist_tier,
    is_eur_dense_rank_tier,
    is_eur_cume_dist_tier,
    cnt,
    cnt_2,
    is_eur_dense_rank_quartiles,
    is_eur_dense_rank_cume_dist_quartiles
from grouped
order by is_eur_dense_rank_cume_dist_tier nulls last
