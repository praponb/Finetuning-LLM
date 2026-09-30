select
    case when year(sale_date) < 2024 then 'before_2024' when year(sale_date) = 2024 then '2024' else 'after_2024' end as sale_cohort,
    avg(units) as avg_units,
    min(revenue) as min_revenue,
    max(revenue) as max_revenue
from store_sales
group by case when year(sale_date) < 2024 then 'before_2024' when year(sale_date) = 2024 then '2024' else 'after_2024' end;
