select date_trunc('MONTH', ss.sale_date) as sale_month, quarter(ss.sale_date) as sale_quarter, sum(i.quantity) as total_quantity
from inventory i
join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
group by date_trunc('MONTH', ss.sale_date), quarter(ss.sale_date);
