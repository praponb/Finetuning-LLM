select id, store_id, date_sub(sale_date, 19) as sale_date_minus_19d
from store_sales
order by sale_date_minus_19d;
