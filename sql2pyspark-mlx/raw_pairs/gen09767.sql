with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        element
    from price_tiers
    lateral view outer explode(discounts) e as element
),
base as (
    -- Keep recent, valid rows
    select *, concat_ws('-', plan, currency) as plan_currency_key
    from flattened
    where element <= 40
),
base_price_tiers as (
    -- Filter the raw rows and derive helper columns
    select
        plan,
        currency,
        element,
        plan_currency_key,
        substring(currency, 1, 3) as currency_prefix,
        substring(currency, 1, 3) as currency_prefix_2
    from base
    where currency <> 'USD'
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        max(element) over (partition by plan) as plan_max_element,
        percent_rank() over (partition by plan order by element desc) as element_percent_rank,
        cume_dist() over (order by element desc nulls first) as element_cume_dist
    from base_price_tiers
    where plan = 'pro'
),
valid as (
    -- Keep recent, valid rows
    select *, coalesce(element_cume_dist, 0) as element_cume_dist_filled
    from metrics
    where plan in ('pro', 'enterprise')
)
-- Grouped metrics
select
    case when element_percent_rank >= 0.8 then 'LARGE' else 'SMALL' end as element_percent_rank_tier,
    size(collect_set(currency)) as num_currencies,
    sum(case when currency = 'EUR' then element_cume_dist else 0 end) as eur_element_cume_dist,
    size(collect_set(plan)) as num_plans,
    count(*) as row_count,
    count(case when plan = 'basic' then 1 end) as basic_count
from valid
group by case when element_percent_rank >= 0.8 then 'LARGE' else 'SMALL' end
order by num_currencies nulls last;
