with rollup_totals as (
    select
        s.city,
        a.brand,
        count(*) as rows_total,
        min(a.cost) as min_cost,
        grouping_id() as grouping_level
    from products a
    left join suppliers s on a.supplier_id = s.id
    group by rollup (s.city, a.brand)
)
select
    city,
    brand,
    rows_total,
    case when city rlike '^(Tokyo|London)$' then 'EXTENDED' else 'PRIORITY' end as city_zone
from rollup_totals
order by brand;
