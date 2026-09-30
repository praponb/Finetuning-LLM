select date_trunc('MONTH', sale_date) as sale_month, max(units) as max_units
from store_sales
group by 1
having max(units) < 30
order by max_units, sale_month
limit 1
