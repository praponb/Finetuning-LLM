SELECT suppliers.city, CASE WHEN suppliers.rating >= 4.0 THEN 'high' WHEN suppliers.rating >= 2.6 THEN 'mid' ELSE 'low' END AS rating_bucket, MAX(store_sales.units) - MIN(store_sales.units) AS units_range
FROM store_sales
INNER JOIN products ON store_sales.product_id = products.id
FULL OUTER JOIN suppliers ON products.supplier_id = suppliers.id
WHERE products.launch_date >= '2023-03-30'
GROUP BY suppliers.city, CASE WHEN suppliers.rating >= 4.0 THEN 'high' WHEN suppliers.rating >= 2.6 THEN 'mid' ELSE 'low' END;
