SELECT products.category, ROUND(STDDEV(store_sales.revenue), 2) stddev_revenue, COUNT(DISTINCT store_sales.revenue) unique_stores
FROM store_sales
INNER JOIN products ON store_sales.product_id = products.id
LEFT JOIN stores ON store_sales.store_id = stores.id
GROUP BY products.category
HAVING ROUND(STDDEV(store_sales.revenue), 2) >= 21
