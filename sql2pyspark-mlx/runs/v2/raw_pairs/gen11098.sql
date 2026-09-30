SELECT CASE WHEN suppliers.rating >= 4.5 THEN 'high' WHEN suppliers.rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, ROUND((100 * SUM(CASE WHEN products.category = 'Grocery' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_grocery, COUNT(CASE WHEN products.brand = 'Umbrella' THEN 1 END) AS umbrella_count
FROM products
JOIN suppliers ON products.supplier_id = suppliers.id
GROUP BY CASE WHEN suppliers.rating >= 4.5 THEN 'high' WHEN suppliers.rating >= 2.1 THEN 'mid' ELSE 'low' END
ORDER BY umbrella_count
