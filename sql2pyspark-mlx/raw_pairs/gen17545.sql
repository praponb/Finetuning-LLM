SELECT CASE WHEN s.sqft >= 17632 THEN 'high' WHEN s.sqft >= 5629 THEN 'mid' ELSE 'low' END AS sqft_bucket, MIN(i.last_restock) AS min_last_restock, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON i.store_id = s.id
GROUP BY CASE WHEN s.sqft >= 17632 THEN 'high' WHEN s.sqft >= 5629 THEN 'mid' ELSE 'low' END
HAVING ROUND(STDDEV(i.quantity), 2) > 73
