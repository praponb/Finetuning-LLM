with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        explode_outer(discounts) as element
    from price_tiers
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        initcap(trim(plan)) as plan_title,
        substring(currency, 1, 3) as currency_prefix
    from flattened
    where element >= 25
),
cleaned as (
    -- Basic cleansing and defaults
    select *, length(plan) as plan_length
    from filtered
    where plan != 'enterprise'
),
filtered_price_tiers as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        coalesce(plan_length, 0) as plan_length_filled,
        trim(plan) as clean_plan
    from cleaned
    where element < 34
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over (partition by currency order by plan_length nulls last) as plan_length_percent_rank,
        max(plan_length) over (partition by plan) as plan_max_plan_length,
        dense_rank() over (partition by currency order by plan_length_filled desc nulls first) as plan_length_filled_dense_rank,
        sum(element) over (partition by plan) as plan_sum_element
    from filtered_price_tiers
)
-- Grouped metrics
select
    currency,
    case when plan_length_percent_rank >= 0.8 then 'PREMIUM' else 'STANDARD' end as plan_length_percent_rank_tier,
    count(*) filter (where plan = 'starter') as starter_rows,
    percentile_approx(plan_max_plan_length, array(0.25, 0.5, 0.75)) as plan_max_plan_length_quartiles,
    count(distinct plan) as distinct_plans,
    round(sum(plan_max_plan_length) / nullif(count(case when currency = 'USD' then 1 end), 0), 2) as plan_max_plan_length_per_usd,
    approx_percentile(plan_length, array(0.25, 0.5, 0.75)) as plan_length_quartiles,
    count(distinct currency) as distinct_currencies,
    rank() over (partition by currency order by count(*) filter (where plan = 'starter') desc) as starter_rows_rank
from metrics
group by currency, case when plan_length_percent_rank >= 0.8 then 'PREMIUM' else 'STANDARD' end
order by currency
