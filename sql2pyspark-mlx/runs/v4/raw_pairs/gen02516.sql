with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        explode(tier_prices) as val
    from price_tiers
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        plan,
        currency,
        val,
        rank() over (partition by plan order by val desc) as val_rank,
        cume_dist() over (order by val desc nulls first) as val_cume_dist,
        dense_rank() over (partition by plan order by val desc nulls first) as val_dense_rank
    from flattened
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, floor(val_rank) as val_rank_whole
    from with_windows
    where currency in ('THB', 'EUR')
)
select
    id,
    plan,
    currency,
    val,
    val_rank,
    val_dense_rank,
    val_rank_whole,
    case when plan rlike '^(starter|enterprise)$' then 'PRIORITY' when plan rlike '^(basic|pro)$' then 'EXTENDED' else 'CORE' end as plan_zone
from cleaned;
