SELECT CASE WHEN suppliers.rating >= 4.2 THEN 'high' WHEN suppliers.rating >= 1.8 THEN 'mid' ELSE 'low' END AS rating_bucket, MAX(store_sales.revenue) AS max_revenue
FROM store_sales
LEFT JOIN products ON store_sales.product_id = products.id
FULL OUTER JOIN suppliers ON products.supplier_id = suppliers.id
GROUP BY CASE WHEN suppliers.rating >= 4.2 THEN 'high' WHEN suppliers.rating >= 1.8 THEN 'mid' ELSE 'low' END;
