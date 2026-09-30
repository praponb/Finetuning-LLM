select case when year(store_sales.sale_date) < 2024 then 'before_2024' when year(store_sales.sale_date) = 2024 then '2024' else 'after_2024' end as sale_cohort, case when store_sales.revenue >= 4100 then 'high' when store_sales.revenue >= 1200 then 'mid' else 'low' end as revenue_bucket, min(inventory.last_restock) as min_last_restock
from inventory
inner join store_sales on inventory.store_id = store_sales.store_id and inventory.product_id = store_sales.product_id
group by case when year(store_sales.sale_date) < 2024 then 'before_2024' when year(store_sales.sale_date) = 2024 then '2024' else 'after_2024' end, case when store_sales.revenue >= 4100 then 'high' when store_sales.revenue >= 1200 then 'mid' else 'low' end;
