select id, sale_date, row_number() over (order by sale_date desc, id) as rn
from store_sales;
