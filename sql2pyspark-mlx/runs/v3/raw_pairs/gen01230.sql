with base as (
    select *
    from products
    where brand is null and supplier_id = 32
),
launch_cohort_summary as (
    select case when year(launch_date) < 2024 then 'before_2024' when year(launch_date) = 2024 then '2024' else 'after_2024' end launch_cohort, concat_ws(', ', sort_array(collect_set(brand))) brand_values
    from base
    group by case when year(launch_date) < 2024 then 'before_2024' when year(launch_date) = 2024 then '2024' else 'after_2024' end
)
select launch_cohort, brand_values
from launch_cohort_summary
order by brand_values desc
