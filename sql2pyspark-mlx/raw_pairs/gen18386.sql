select *
from (
    select
        case when city in ('Bangkok', 'Tokyo') then 'priority' else 'other' end as city_group,
        city,
        sum(sqft) as total_sqft,
        min(opened_date) as first_opened_date
    from stores
    group by case when city in ('Bangkok', 'Tokyo') then 'priority' else 'other' end, city
) as sub
where total_sqft < 78000;
