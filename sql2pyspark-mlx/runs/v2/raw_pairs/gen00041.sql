select quarter(sale_date) as sale_quarter, round(avg(units), 2) as avg_units
from store_sales
group by quarter(sale_date)
