select month(ss.sale_date) as sale_month, max(i.quantity) as max_quantity, count(distinct i.quantity) as unique_products
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
group by month(ss.sale_date)
order by sale_month
