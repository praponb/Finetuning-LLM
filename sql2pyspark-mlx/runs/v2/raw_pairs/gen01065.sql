select store_id, max(sale_date) as last_sale_date
from store_sales
group by 1
order by last_sale_date desc
