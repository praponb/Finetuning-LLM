select id, revenue, case when units >= 44 then 'High' when units >= 12 then 'Medium' else 'Low' end as units_band
from store_sales
where product_id = 1 and sale_date between '2024-09-01' and '2024-11-18'
order by units_band desc, revenue
