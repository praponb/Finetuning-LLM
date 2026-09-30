select products.category, min(store_sales.revenue) as min_revenue, count(distinct store_sales.units) as unique_products
from store_sales
full outer join products on store_sales.product_id = products.id
left outer join stores on stores.id = store_sales.store_id
group by products.category
