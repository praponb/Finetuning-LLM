select products.category, min(store_sales.units) as min_units, min(store_sales.revenue) as min_revenue
from store_sales
full outer join products on store_sales.product_id = products.id
where store_sales.store_id in (1, 4, 6)
group by products.category
order by min_revenue desc
