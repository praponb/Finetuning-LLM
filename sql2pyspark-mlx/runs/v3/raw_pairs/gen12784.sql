select date_format(ss.sale_date, 'yyyy-MM') as sale_month, sum(i.quantity) as total_quantity, round(avg(i.quantity), 2) as avg_quantity
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.product_id = 10
group by date_format(ss.sale_date, 'yyyy-MM')
order by total_quantity, avg_quantity desc, sale_month
limit 3;
