with filtered as (
    -- Keep recent, valid rows
    select *, add_months(starts_on, 6) starts_on_plus_months
    from promotions
    where starts_on >= date '2023-08-01'
),
scored as (
    -- Running and moving aggregates
    select
        id,
        starts_on,
        discount_pct,
        rank() over (partition by promo_code order by discount_pct desc) discount_pct_rank,
        ntile(3) over (order by discount_pct, id) discount_pct_bucket,
        stddev_samp(discount_pct) over (partition by promo_code) promo_code_stddev_samp_discount_pct,
        count(discount_pct) over (partition by month(ends_on), channel) ends_on_month_count_discount_pct,
        first_value(channel) ignore nulls over w_grp first_known_channel
    from filtered
    window w_grp as (partition by channel, promo_code order by starts_on, id rows between unbounded preceding and current row)
),
filtered_promotions as (
    -- Keep recent, valid rows
    select
        id,
        starts_on,
        discount_pct,
        discount_pct_bucket,
        promo_code_stddev_samp_discount_pct,
        ends_on_month_count_discount_pct,
        first_known_channel,
        greatest(discount_pct_rank, promo_code_stddev_samp_discount_pct) greatest_discount_pct_rank_promo_code_stddev_samp_discount_pct
    from scored
    where discount_pct_rank between 3 and 5
),
filtered_promotions_2 as (
    -- Filter the raw rows and derive helper columns
    select *, ceil(discount_pct) discount_pct_whole
    from filtered_promotions
    where greatest_discount_pct_rank_promo_code_stddev_samp_discount_pct <= 32
),
base as (
    -- Basic cleansing and defaults
    select *, add_months(starts_on, 1) starts_on_plus_months
    from filtered_promotions_2
    where ends_on_month_count_discount_pct <= 13
)
-- Multi-level aggregation
select
    date_trunc('month', starts_on_plus_months) as activity_month,
    approx_percentile(discount_pct_whole, 0.9) as discount_pct_whole_p90,
    sum(discount_pct_whole) as total_discount_pct_whole,
    count(*) as event_count
from base
group by date_trunc('month', starts_on_plus_months)
order by discount_pct_whole_p90
