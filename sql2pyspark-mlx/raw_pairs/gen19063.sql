SELECT p.brand, COUNT(i.id) AS inventory_count
FROM products p
LEFT OUTER JOIN inventory i ON p.id = i.product_id
GROUP BY p.brand
HAVING COUNT(i.id) > 5
ORDER BY inventory_count, brand
LIMIT 5;
