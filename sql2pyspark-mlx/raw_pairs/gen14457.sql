with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        element
    from price_tiers
    lateral view explode(discounts) li as element
),
with_windows as (
    -- Running and moving aggregates
    select
        plan,
        effective_from,
        element,
        rank() over (partition by plan order by element desc nulls first) as element_rank,
        percent_rank() over (order by element nulls first) as element_percent_rank
    from lines
),
kept_keys as (
    -- Set difference on keys
    select plan
    from with_windows
    except
    select plan
    from price_tiers
    where currency = 'USD'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    inner join kept_keys k on s.plan = k.plan
),
remaining_keys as (
    -- Set difference on keys
    select plan
    from verified
    except
    select plan
    from price_tiers
    where currency = 'USD'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from verified s
    inner join remaining_keys k on s.plan = k.plan
)
-- Grouped metrics
select
    case when element >= 71 then 'PREMIUM' else 'STANDARD' end as element_tier,
    sum(element) filter (where plan = 'basic') as basic_total_element,
    size(collect_set(plan)) as num_plans,
    approx_percentile(element_percent_rank, 0.95) as element_percent_rank_p95
from kept_rows
group by element_tier
order by element_percent_rank_p95 desc nulls first;
