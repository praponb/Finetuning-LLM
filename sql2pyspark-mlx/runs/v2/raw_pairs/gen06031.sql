select date_trunc('MONTH', p.launch_date) as launch_month, count(distinct ss.revenue) as unique_products, avg(ss.units) as avg_units
from store_sales ss
right join products p on ss.product_id = p.id
group by date_trunc('MONTH', p.launch_date)
order by avg_units
