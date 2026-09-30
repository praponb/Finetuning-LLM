select sale_date, date_trunc('WEEK', sale_date) as sale_date_period
from store_sales
order by sale_date_period desc
