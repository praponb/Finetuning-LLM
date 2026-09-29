with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        explode(discounts) as element
    from price_tiers
),
enriched as (
    -- Window metrics over several different partitions
    select *, cume_dist() over (partition by currency order by element desc nulls last) as element_cume_dist
    from exploded
    where currency in ('EUR', 'THB')
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, coalesce(element_cume_dist, 0) as element_cume_dist_filled
    from enriched
    where element_cume_dist between 0.4 and 0.59
),
metrics as (
    -- Window metrics over several different partitions
    select *, rank() over (partition by plan order by element_cume_dist_filled nulls last) as element_cume_dist_filled_rank
    from base
),
enriched_price_tiers as (
    -- Running and moving aggregates
    select
        plan,
        element_cume_dist_filled_rank,
        cume_dist() over (partition by plan order by element desc) as element_cume_dist_2
    from metrics
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        element_cume_dist_2 - element_cume_dist_filled_rank as element_cume_dist_2_minus_element_cume_dist_filled_rank,
        floor(element_cume_dist_filled_rank) as element_cume_dist_filled_rank_whole
    from enriched_price_tiers
    where element_cume_dist_2 >= 0.21
)
-- Grouped metrics
select
    plan,
    size(collect_set(plan)) as num_plans,
    count(*) filter (where plan = 'starter') as starter_rows,
    count(case when plan = 'basic' then 1 end) as basic_count,
    count(distinct plan) as distinct_plans,
    sum(element_cume_dist_filled_rank_whole) as total_element_cume_dist_filled_rank_whole,
    round(avg(element_cume_dist_2_minus_element_cume_dist_filled_rank), 2) as avg_element_cume_dist_2_minus_element_cume_dist_filled_rank
from filtered
group by 1
having starter_rows < 1000
order by plan desc nulls first;
