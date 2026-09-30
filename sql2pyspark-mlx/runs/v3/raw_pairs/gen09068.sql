select date_trunc('MONTH', ss.sale_date) sale_month, ss.product_id, count(distinct i.quantity) unique_stores
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
group by date_trunc('MONTH', ss.sale_date), ss.product_id
order by product_id desc
