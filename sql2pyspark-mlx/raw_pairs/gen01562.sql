with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        explode_outer(discounts) as element
    from price_tiers
),
scored as (
    -- Running and moving aggregates
    select
        *,
        max(element) over (partition by year(effective_from), currency) as effective_from_year_max_element,
        dense_rank() over (partition by date_trunc('WEEK', effective_from), plan order by element desc nulls last) as element_dense_rank,
        max(element) over (partition by currency) as currency_max_element,
        sum(element) over (partition by plan, currency) as plan_sum_element
    from lines
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        plan,
        effective_from,
        currency,
        element,
        effective_from_year_max_element,
        element_dense_rank,
        currency_max_element,
        plan_sum_element,
        length(plan) as plan_length
    from scored
    where effective_from >= date '2024-04-14'
      and effective_from <= '2023-04-01'
),
kept_keys as (
    -- Set intersection on keys
    select currency, id
    from base
    intersect all
    select currency, id
    from price_tiers
    where effective_from > '2025-08-26'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    join kept_keys k on s.currency = k.currency and s.id = k.id
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        plan,
        effective_from,
        currency,
        effective_from_year_max_element,
        element_dense_rank,
        plan_sum_element,
        concat(currency, ', ', plan) as currency_plan
    from kept_rows
    where effective_from >= date '2024-05-23'
)
-- Ranking and gap features
select
    id,
    plan,
    effective_from,
    element_dense_rank,
    rank() over w_running as effective_from_year_max_element_rank,
    dense_rank() over (partition by currency, plan order by effective_from_year_max_element desc) as effective_from_year_max_element_dense_rank
from filtered
window w_running as (order by effective_from_year_max_element)
