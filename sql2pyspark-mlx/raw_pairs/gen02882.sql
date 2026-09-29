with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        val
    from price_tiers
    lateral view explode(tier_prices) x as val
),
metrics as (
    -- Running and moving aggregates
    select
        plan,
        currency,
        val,
        min(val) over w_hist as currency_min_val,
        cume_dist() over w_hist_2 as val_cume_dist,
        stddev_samp(val) over w_hist as currency_stddev_samp_val,
        count(val) over (partition by plan, currency) as plan_count_val,
        cume_dist() over w_hist_3 as val_cume_dist_2
    from exploded
    window w_hist as (partition by currency), w_hist_2 as (partition by plan order by val desc nulls first), w_hist_3 as (partition by plan order by val desc)
),
filtered as (
    -- Keep recent, valid rows
    select
        plan,
        currency,
        val,
        currency_min_val,
        currency_stddev_samp_val,
        val_cume_dist_2,
        floor(currency_stddev_samp_val) as currency_stddev_samp_val_whole
    from metrics
    where val_cume_dist_2 between 0.36 and 0.54
),
filtered_price_tiers as (
    -- Basic cleansing and defaults
    select
        currency,
        val,
        currency_min_val,
        currency_stddev_samp_val,
        currency_stddev_samp_val_whole,
        floor(currency_stddev_samp_val_whole) as currency_stddev_samp_val_whole_whole
    from filtered
    where currency_stddev_samp_val_whole between 36 and 53
)
-- Grouped metrics
select
    case when val >= 62 then 'PREMIUM' else 'STANDARD' end as val_tier,
    case when currency_stddev_samp_val_whole >= 82 then 'PREMIUM' else 'STANDARD' end as currency_stddev_samp_val_whole_tier,
    currency,
    sort_array(collect_set(currency)) as currency_list,
    avg(currency_stddev_samp_val) as avg_currency_stddev_samp_val,
    sum(case when currency = 'EUR' then currency_stddev_samp_val_whole_whole else 0 end) as eur_currency_stddev_samp_val_whole_whole,
    sort_array(collect_set(currency)) as currency_list_2,
    percentile_approx(val, 0.9) as val_p90
from filtered_price_tiers
group by case when val >= 62 then 'PREMIUM' else 'STANDARD' end, case when currency_stddev_samp_val_whole >= 82 then 'PREMIUM' else 'STANDARD' end, currency
having val_p90 > 0;
