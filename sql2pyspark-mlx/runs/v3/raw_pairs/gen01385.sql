SELECT CASE WHEN p.price >= 950 THEN 'high' WHEN p.price >= 330 THEN 'mid' ELSE 'low' END AS price_bucket, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
RIGHT JOIN products p ON i.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE p.supplier_id IS NOT NULL
GROUP BY CASE WHEN p.price >= 950 THEN 'high' WHEN p.price >= 330 THEN 'mid' ELSE 'low' END
ORDER BY 1;
