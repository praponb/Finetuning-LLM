SELECT CASE WHEN suppliers.rating >= 4.5 THEN 'high' WHEN suppliers.rating >= 1.5 THEN 'mid' ELSE 'low' END AS rating_bucket, MIN(store_sales.revenue) AS min_revenue
FROM store_sales
LEFT JOIN products ON store_sales.product_id = products.id
INNER JOIN suppliers ON products.supplier_id = suppliers.id
GROUP BY CASE WHEN suppliers.rating >= 4.5 THEN 'high' WHEN suppliers.rating >= 1.5 THEN 'mid' ELSE 'low' END
HAVING MIN(store_sales.revenue) >= 3700
ORDER BY min_revenue, rating_bucket
