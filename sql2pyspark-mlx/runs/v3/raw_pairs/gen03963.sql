select case when year(sale_date) < 2024 then 'before_2024' when year(sale_date) = 2024 then '2024' else 'after_2024' end sale_cohort, sum(units) total_units, rank() over (order by sum(units) desc) as units_rank
from store_sales
where product_id in (1, 3)
group by case when year(sale_date) < 2024 then 'before_2024' when year(sale_date) = 2024 then '2024' else 'after_2024' end
order by total_units, sale_cohort
limit 1;
