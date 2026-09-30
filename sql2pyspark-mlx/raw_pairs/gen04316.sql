SELECT CASE WHEN suppliers.rating >= 4.4 THEN 'high' WHEN suppliers.rating >= 1.5 THEN 'mid' ELSE 'low' END AS rating_bucket, MAX(store_sales.sale_date) AS latest_sale_date
FROM store_sales
JOIN products ON store_sales.product_id = products.id
LEFT OUTER JOIN suppliers ON products.supplier_id = suppliers.id
GROUP BY CASE WHEN suppliers.rating >= 4.4 THEN 'high' WHEN suppliers.rating >= 1.5 THEN 'mid' ELSE 'low' END
ORDER BY latest_sale_date DESC, rating_bucket DESC
LIMIT 10
