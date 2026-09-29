select sale_cohort, cnt
from (
    select case when year(sale_date) < 2024 then 'before_2024' when year(sale_date) = 2024 then '2024' else 'after_2024' end sale_cohort, count(*) cnt
    from store_sales
    group by case when year(sale_date) < 2024 then 'before_2024' when year(sale_date) = 2024 then '2024' else 'after_2024' end
) x
where cnt >= 9;
