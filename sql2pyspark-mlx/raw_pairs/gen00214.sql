select units, revenue
from store_sales
where sale_date < '2023-09-09' or product_id in (2, 3, 6)
order by units asc
