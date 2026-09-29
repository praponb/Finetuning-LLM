with long_format as (
    -- Native SQL UNPIVOT (wide to long)
    select *
    from (
        select
            id,
            supplier_id,
            cost,
            price
        from products
    ) src
    unpivot (
        reading for measure in (cost, price)
    )
)
-- Aggregate with conditional metrics
select
    case when reading >= 75 then 'LARGE' else 'SMALL' end as reading_tier,
    size(collect_set(measure)) as num_measures,
    count(distinct measure) as distinct_measures,
    sort_array(collect_set(measure)) as measure_list
from long_format
group by 1
having size(collect_set(measure)) >= 1;
