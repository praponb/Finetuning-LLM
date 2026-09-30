with dim_totals as (
    -- Join a dimension and aggregate
    select
        dim.region,
        dim.city,
        count(*) as cnt,
        sum(src.units) as total_units,
        round(sum(src.revenue), 2) as total_revenue
    from store_sales src
    join stores dim on src.store_id = dim.id
    group by grouping sets ((dim.region, dim.city), (dim.region))
)
select
    region,
    city,
    cnt,
    total_revenue,
    case when region rlike '^(Central|South)$' then 'EXTENDED' when region rlike '^(North|East)$' then 'GROUP_B' else 'PRIORITY' end as region_zone
from dim_totals
order by city desc nulls first
