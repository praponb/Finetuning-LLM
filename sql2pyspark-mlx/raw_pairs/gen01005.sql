with exploded as (
    select
        id,
        plan,
        effective_from,
        currency,
        element
    from price_tiers
    lateral view explode(discounts) t as element
),
filtered as (
    select
        plan,
        effective_from,
        currency,
        element,
        case when currency in ('USD', 'THB') then 'Core' else 'Other' end as currency_group
    from exploded
    where effective_from > date '2024-05-12'
),
cleaned as (
    select
        *,
        date_trunc('year', effective_from) as effective_from_period,
        trim(plan) as clean_plan
    from filtered
    where plan in ('basic', 'enterprise', 'starter')
),
metrics as (
    select
        plan,
        element,
        currency_group,
        effective_from_period,
        clean_plan,
        max(element) over w_ordered as element_max_30d,
        percent_rank() over (order by element nulls first) as element_percent_rank,
        dense_rank() over (partition by plan order by element nulls first) as element_dense_rank
    from cleaned
    where to_date(effective_from_period) < '2023-03-01'
    window w_ordered as (partition by plan order by cast(effective_from_period as bigint) range between 2592000 preceding and current row)
),
with_windows as (
    select
        element,
        effective_from_period,
        clean_plan,
        count(element_dense_rank) over w_running as plan_count_element_dense_rank,
        sum(element) over (partition by year(effective_from_period), plan) as effective_from_period_year_sum_element,
        rank() over w_ordered as element_percent_rank_rank
    from metrics
    window w_running as (partition by plan), w_ordered as (partition by plan order by element_percent_rank desc)
)
select
    element,
    clean_plan,
    plan_count_element_dense_rank,
    effective_from_period_year_sum_element
from with_windows
order by plan_count_element_dense_rank desc;
