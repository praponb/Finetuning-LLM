select id, units, dense_rank() over (order by units) units_dense_rank
from store_sales
where sale_date < '2024-10-27'
