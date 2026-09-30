with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        element
    from price_tiers
    lateral view outer explode(discounts) e as element
),
base as (
    -- Keep recent, valid rows
    select *, initcap(trim(plan)) as plan_title
    from flattened
    where effective_from >= date '2024-06-06'
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        plan,
        effective_from,
        element,
        plan_title,
        case when plan in ('basic', 'enterprise') then 'Priority' else 'Other' end as plan_group,
        length(plan) as plan_length
    from base
    where effective_from > to_date('2024-06-02')
      and month(effective_from) in (6, 7, 11)
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, round(plan_length, 0) as plan_length_rounded
    from filtered
    where effective_from >= date '2024-04-15'
      and element < 24
)
select
    id,
    plan,
    effective_from,
    element,
    plan_group,
    plan_length
from cleaned;
