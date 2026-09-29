select case when year(last_restock) < 2024 then 'before_2024' when year(last_restock) = 2024 then '2024' else 'after_2024' end as last_restock_cohort, round(stddev(quantity), 2) as stddev_quantity, round(avg(quantity), 2) as avg_quantity
from inventory
where store_id = 2 and product_id in (1, 10)
group by 1
order by last_restock_cohort desc
