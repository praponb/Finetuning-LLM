select
    case when year(sale_date) < 2024 then 'before_2024' when year(sale_date) = 2024 then '2024' else 'after_2024' end sale_cohort,
    year(sale_date) sale_year,
    count(*) total_count,
    min(revenue) min_revenue,
    count(distinct units) unique_products
from store_sales
group by sale_cohort, sale_year
having count(*) > 5;
