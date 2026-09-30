select max(sale_date) as last_sale_date
from store_sales
order by last_sale_date
