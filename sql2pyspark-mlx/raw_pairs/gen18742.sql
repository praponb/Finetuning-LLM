with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        explode(discounts) as val
    from price_tiers
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when plan = 'starter' then 1 else 0 end as is_starter,
        initcap(trim(plan)) as plan_title
    from flattened
    where val <= 62
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        cast(val as int) as val_int,
        substring(plan, 1, 4) as plan_prefix
    from cleaned
    where is_starter = 58
),
cleaned_price_tiers as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        trim(plan) as clean_plan,
        ceil(val_int) as val_int_whole
    from filtered
    where val_int between 30 and 50
)
select
    id,
    plan,
    val,
    is_starter,
    plan_title,
    val_int,
    plan_prefix,
    clean_plan
from cleaned_price_tiers
order by is_starter;
