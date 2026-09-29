with rollup_totals as (
    -- Join a dimension and aggregate
    select
        s.city,
        src.brand,
        count(*) as cnt,
        round(sum(src.price), 2) as total_price,
        grouping_id() as grouping_level
    from products src
    inner join suppliers s on src.supplier_id = s.id
    group by cube (s.city, src.brand)
)
select
    city,
    brand,
    total_price,
    grouping_level,
    case when city rlike '^(New York|Berlin)$' then 'PRIORITY' when city rlike '^(Bangkok|Chiang Mai)$' then 'OTHER_ZONE' else 'GROUP_A' end as city_zone
from rollup_totals;
