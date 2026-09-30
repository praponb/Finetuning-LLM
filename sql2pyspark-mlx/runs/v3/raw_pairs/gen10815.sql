select
    store_id,
    case when year(sale_date) < 2024 then 'before_2024' when year(sale_date) = 2024 then '2024' else 'after_2024' end as sale_cohort,
    count(distinct revenue) as unique_products,
    round(stddev(units), 2) as stddev_units
from store_sales
where revenue >= 3900 and month(sale_date) in (3, 4, 10)
group by store_id, case when year(sale_date) < 2024 then 'before_2024' when year(sale_date) = 2024 then '2024' else 'after_2024' end
