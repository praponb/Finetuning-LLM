with valid_store_sales as (
    select *, coalesce(revenue, 0) as revenue_filled
    from store_sales
    where product_id in (4, 6, 7, 8)
),
store_sales_stats as (
    select case when year(sale_date) < 2024 then 'before_2024' when year(sale_date) = 2024 then '2024' else 'after_2024' end as sale_cohort, avg(revenue) as avg_revenue, count(distinct units) as unique_stores
    from valid_store_sales
    group by case when year(sale_date) < 2024 then 'before_2024' when year(sale_date) = 2024 then '2024' else 'after_2024' end
)
select sale_cohort, unique_stores
from store_sales_stats
order by unique_stores desc, sale_cohort desc
