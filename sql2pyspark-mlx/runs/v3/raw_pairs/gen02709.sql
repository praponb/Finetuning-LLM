select product_id, max(revenue) as max_revenue, min(revenue) as min_revenue
from store_sales
group by product_id
having max_revenue >= 3900
order by product_id desc
limit 50
