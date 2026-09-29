with cleaned as (
    -- Keep recent, valid rows
    select *, cast(quantity as double) as quantity_str
    from inventory
    where store_id in (3, 4)
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by store_id, product_id order by last_restock) as rn
        from cleaned
    ) ranked
    where rn = 1
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, dayofweek(last_restock) as last_restock_dayofweek
    from deduped
    where last_restock >= date '2023-10-04'
),
summary as (
    -- Multi-level aggregation
    select
        case when quantity_str >= 68 then 'LARGE' else 'SMALL' end as quantity_str_tier,
        count(*) as event_count,
        round(avg(last_restock_dayofweek), 2) as avg_last_restock_dayofweek,
        percentile_approx(quantity_str, 0.5) as quantity_str_p50,
        percentile_approx(quantity_str, 0.9) as quantity_str_p90,
        percentile_approx(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles,
        count(distinct product_id) as distinct_product_ids
    from valid
    group by case when quantity_str >= 68 then 'LARGE' else 'SMALL' end
)
select
    quantity_str_tier,
    event_count,
    avg_last_restock_dayofweek,
    quantity_str_p50,
    quantity_str_p90,
    quantity_quartiles,
    distinct_product_ids
from summary
order by quantity_str_p50 nulls first
