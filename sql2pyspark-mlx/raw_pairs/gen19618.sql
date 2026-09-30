with filtered_store_sales as (
    select *
    from store_sales
    where year(sale_date) = 2025
),
sale_cohort_summary as (
    select case when year(sale_date) < 2024 then 'before_2024' when year(sale_date) = 2024 then '2024' else 'after_2024' end as sale_cohort, round(avg(revenue), 2) as avg_revenue
    from filtered_store_sales
    group by case when year(sale_date) < 2024 then 'before_2024' when year(sale_date) = 2024 then '2024' else 'after_2024' end
)
select *
from sale_cohort_summary
