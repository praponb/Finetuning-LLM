SELECT CASE WHEN p.price >= 1160 THEN 'high' WHEN p.price >= 370 THEN 'mid' ELSE 'low' END AS price_bucket, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
FULL OUTER JOIN products p ON p.id = i.product_id
LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id
WHERE s.city = 'Tokyo'
GROUP BY CASE WHEN p.price >= 1160 THEN 'high' WHEN p.price >= 370 THEN 'mid' ELSE 'low' END
HAVING ROUND(STDDEV(i.quantity), 2) >= 33;
