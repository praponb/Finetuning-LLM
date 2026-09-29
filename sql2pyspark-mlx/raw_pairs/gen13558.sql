with base as (
    select *
    from products
    where cost >= 650 and launch_date < '2025-02-01'
),
summary as (
    select
        brand,
        case when year(launch_date) < 2024 then 'before_2024' when year(launch_date) = 2024 then '2024' else 'after_2024' end as launch_cohort,
        sort_array(collect_set(brand)) as brand_list,
        max(price) - min(price) as price_range
    from base
    group by brand, case when year(launch_date) < 2024 then 'before_2024' when year(launch_date) = 2024 then '2024' else 'after_2024' end
)
select brand, launch_cohort, brand_list
from summary
where price_range > 22
